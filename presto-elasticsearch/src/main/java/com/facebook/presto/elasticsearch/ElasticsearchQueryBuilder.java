/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_UNKNOWN_SEARCH_NODE;
import static com.facebook.presto.spi.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ElasticsearchQueryBuilder
{
    private static final Logger log = Logger.get(ElasticsearchQueryBuilder.class);

    private final Duration scrollTime;
    private final int scrollSize;
    private final Client client;
    private final int shard;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final List<ElasticsearchColumnHandle> columns;
    private final String index;
    private final String type;

    public ElasticsearchQueryBuilder(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        columns = requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(config, "config is null");
        requireNonNull(split, "split is null");
        ElasticsearchTableDescription table = split.getTable();
        tupleDomain = split.getTupleDomain();
        index = table.getIndex();
        type = table.getType();
        shard = split.getShard();
        try {
            Settings settings = Settings.builder().put("cluster.name", table.getClusterName()).build();
            client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(split.getSearchNode()), table.getPort()));
        }
        catch (IOException e) {
            throw new PrestoException(ELASTIC_SEARCH_UNKNOWN_SEARCH_NODE, "Error connecting to SearchNode: " + split.getSearchNode() + " with port: " + table.getPort(), e);
        }
        scrollTime = config.getScrollTime();
        scrollSize = config.getScrollSize();
    }

    public SearchRequestBuilder buildScrollSearchRequest()
    {
        String indices = index != null && !index.isEmpty() ? index : "_all";
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setScroll(new TimeValue(scrollTime.toMillis()))
                .setQuery(buildSearchQuery())
                .setPreference("_shards:" + shard)
                .setSize(scrollSize);
        log.debug("ElasticSearch Request: " + searchRequestBuilder.toString());
        return searchRequestBuilder;
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId)
    {
        return client.prepareSearchScroll(scrollId).setScroll(new TimeValue(scrollTime.toMillis()));
    }

    private QueryBuilder buildSearchQuery()
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (ElasticsearchColumnHandle column : columns) {
            Type type = column.getColumnType();
            tupleDomain.getDomains().ifPresent((domains) -> {
                Domain domain = domains.get(column);
                if (domain != null) {
                    boolQueryBuilder.must(buildPredicate(column.getColumnJsonPath(), domain, type, column.getIsList()));
                }
            });
        }
        return boolQueryBuilder.hasClauses() ? boolQueryBuilder : new MatchAllQueryBuilder();
    }

    private QueryBuilder buildPredicate(String columnName, Domain domain, Type type, boolean isList)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            boolQueryBuilder.mustNot(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        if (domain.getValues().isAll()) {
            boolQueryBuilder.must(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        if (isList) {
            return buildMatchQuery(boolQueryBuilder, columnName, domain, type);
        }
        return buildTermQuery(boolQueryBuilder, columnName, domain, type);
    }

    private QueryBuilder buildMatchQuery(BoolQueryBuilder queryBuilder, String columnName, Domain domain, Type type)
    {
        Set<Object> valuesToInclude = new HashSet<>();
        Set<Object> valuesToExclude = new HashSet<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll(), "Predicate could not be empty");
            if (range.isSingleValue()) {
                valuesToInclude.add(range.getLow().getValue());
            }

            if (!range.getLow().isLowerUnbounded() && range.getLow().getBound() == ABOVE) {
                Object value = range.getLow().getValue();
                if (!valuesToExclude.contains(value)) {
                    valuesToExclude.add(value);
                }
            }

            if (!range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == BELOW) {
                Object value = range.getHigh().getValue();
                if (!valuesToExclude.contains(value)) {
                    valuesToExclude.add(value);
                }
            }
        }

        for (Object value : valuesToExclude) {
            queryBuilder.mustNot(new MatchQueryBuilder(columnName, getValue(type, value)));
        }

        if (valuesToInclude.size() == 1) {
            queryBuilder.must(new MatchQueryBuilder(columnName, getValue(type, getOnlyElement(valuesToInclude))));
        }
        else {
            for (Object value : valuesToInclude) {
                queryBuilder.should(new MatchQueryBuilder(columnName, getValue(type, value)));
            }
        }
        return queryBuilder;
    }

    private QueryBuilder buildTermQuery(BoolQueryBuilder queryBuilder, String columnName, Domain domain, Type type)
    {
        List<Object> values = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll(), "Predicate could not be empty");
            if (range.isSingleValue()) {
                values.add(range.getLow().getValue());
                continue;
            }

            if (!range.getLow().isLowerUnbounded()) {
                switch (range.getLow().getBound()) {
                    case ABOVE:
                        queryBuilder.must(new RangeQueryBuilder(columnName).gt(getValue(type, range.getLow().getValue())));
                        break;
                    case EXACTLY:
                        queryBuilder.must(new RangeQueryBuilder(columnName).gte(getValue(type, range.getLow().getValue())));
                        break;
                    case BELOW:
                        throw new IllegalArgumentException("Low marker should never use BELOW bound");
                    default:
                        throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                }
            }
            if (!range.getHigh().isUpperUnbounded()) {
                switch (range.getHigh().getBound()) {
                    case EXACTLY:
                        queryBuilder.must(new RangeQueryBuilder(columnName).lte(getValue(type, range.getHigh().getValue())));
                        break;
                    case BELOW:
                        queryBuilder.must(new RangeQueryBuilder(columnName).lt(getValue(type, range.getHigh().getValue())));
                        break;
                    case ABOVE:
                        throw new IllegalArgumentException("High marker should never use ABOVE bound");
                    default:
                        throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                }
            }
        }
        if (values.size() == 1) {
            queryBuilder.must(new TermQueryBuilder(columnName, getValue(type, getOnlyElement(values))));
        }
        return queryBuilder;
    }

    public static Object getValue(Type type, Object value)
    {
        if (type.equals(BIGINT)) {
            return value;
        }
        else if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        else if (type.equals(DOUBLE)) {
            return value;
        }
        else if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        else if (type.equals(BOOLEAN)) {
            return value;
        }
        throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
    }
}
