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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final ElasticsearchTableDescription table;
    private final int shard;
    private final String searchNode;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("table") ElasticsearchTableDescription table,
            @JsonProperty("shard") int shard,
            @JsonProperty("searchNode") String searchNode,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.shard = shard;
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.table = requireNonNull(table, "table is null");
        this.searchNode = requireNonNull(searchNode, "searchNode is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ElasticsearchTableDescription getTable()
    {
        return table;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public String getSearchNode()
    {
        return searchNode;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(HostAddress.fromParts(searchNode, table.getPort()));
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(connectorId)
                .addValue(table.getSchemaName())
                .addValue(table.getTableName())
                .addValue(table.getHostAddress())
                .addValue(table.getPort())
                .addValue(table.getClusterName())
                .addValue(table.getIndex())
                .addValue(shard)
                .addValue(searchNode)
                .addValue(tupleDomain)
                .toString();
    }
}
