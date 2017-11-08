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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_MAPPING_REQUEST_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    private static final Logger log = Logger.get(ElasticsearchClient.class);

    private final Map<SchemaTableName, ElasticsearchTableDescription> tableDescriptions;
    private final Map<String, Client> clients = new HashMap<>();
    private final Duration timeout;

    @Inject
    public ElasticsearchClient(Map<SchemaTableName, ElasticsearchTableDescription> descriptions, Duration requestTimeout)
            throws IOException
    {
        tableDescriptions = requireNonNull(descriptions, "Elasticsearch Table Description is null");
        timeout = requireNonNull(requestTimeout, "Elasticsearch request timeout is null");

        for (Map.Entry<SchemaTableName, ElasticsearchTableDescription> entry : tableDescriptions.entrySet()) {
            ElasticsearchTableDescription tableDescription = entry.getValue();
            if (!clients.containsKey(tableDescription.getClusterName())) {
                Settings settings = Settings.builder().put("cluster.name", tableDescription.getClusterName()).build();
                TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(tableDescription.getHostAddress()), tableDescription.getPort()));
                clients.put(tableDescription.getClusterName(), client);
            }
        }
    }

    public List<String> listSchemas()
    {
        return tableDescriptions.keySet().stream().map(SchemaTableName::getSchemaName).collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        return tableDescriptions.keySet()
                .stream()
                .filter(schemaTableName -> schemaNameOrNull == null || schemaTableName.getSchemaName().equals(schemaNameOrNull))
                .collect(toImmutableList());
    }

    public ElasticsearchTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        for (Map.Entry<SchemaTableName, ElasticsearchTableDescription> entry : tableDescriptions.entrySet()) {
            buildColumns(entry.getValue());
        }
        return tableDescriptions.get(new SchemaTableName(schemaName, tableName));
    }

    public ClusterSearchShardsResponse getSearchShards(ElasticsearchTableDescription tableDescription)
    {
        Client client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client cannot be null");
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(tableDescription.getIndex());
        return client.admin().cluster().searchShards(request).actionGet(timeout.toMillis());
    }

    private void buildColumns(ElasticsearchTableDescription tableDescription)
    {
        Client client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client cannot be null");
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(tableDescription.getType());
        if (!isNullOrEmpty(tableDescription.getIndex())) {
            mappingsRequest.indices(tableDescription.getIndex());
        }
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client.admin()
                .indices()
                .getMappings(mappingsRequest)
                .actionGet(timeout.toMillis())
                .getMappings();
        populateColumns(tableDescription, mappings);
    }

    private void populateColumns(ElasticsearchTableDescription tableDescription, ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings)
    {
        Set<ElasticsearchColumn> columns = new HashSet();
        Iterator<String> indexIterator = mappings.keysIt();
        try {
            while (indexIterator.hasNext()) {
                MappingMetaData mappingMetaData = mappings.get(indexIterator.next()).get(tableDescription.getType());
                JSONObject mappingObject = new JSONObject(mappingMetaData.source().toString()).getJSONObject(tableDescription.getType());
                JSONObject properties = mappingObject.getJSONObject("properties");
                List<String> lists = new ArrayList<>();
                if (properties.has("_meta")) {
                    JSONObject meta = mappingObject.getJSONObject("_meta");
                    if (meta.has("lists")) {
                        JSONArray arrays = meta.getJSONArray("lists");
                        for (int i = 0; i < arrays.length(); i++) {
                            lists.add(arrays.getString(i));
                        }
                    }
                }

                for (String columnMetadata : getAllColumnMetadata(null, properties)) {
                    Optional<ElasticsearchColumn> column = createColumn(columnMetadata, lists);
                    if (!column.isPresent()) {
                        continue;
                    }
                    if (columns.stream().noneMatch(col -> col.getName().equals(column.get().getName()))) {
                        columns.add(column.get());
                    }
                }
            }
            columns.add(createColumn("_id.type:string", ImmutableList.of()).get());
            columns.add(createColumn("_index.type:string", ImmutableList.of()).get());
            tableDescription.setColumnsMetadata(columns.stream().map(ElasticsearchColumnMetadata::new).collect(Collectors.toList()));
        }
        catch (JSONException e) {
            throw new PrestoException(ELASTIC_SEARCH_MAPPING_REQUEST_ERROR, e);
        }
    }

    private List<String> getAllColumnMetadata(String parent, JSONObject json)
            throws JSONException
    {
        List<String> metadata = new ArrayList();
        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object child = json.get(key);
            StringBuilder childKey = parent == null || parent.isEmpty() ? new StringBuilder(key) : new StringBuilder(parent).append(".").append(key);

            if (child instanceof JSONObject) {
                metadata.addAll(getAllColumnMetadata(childKey.toString(), (JSONObject) child));
                continue;
            }

            if (!(child instanceof JSONArray)) {
                metadata.add(childKey.append(":").append(child.toString()).toString());
            }
        }
        return metadata;
    }

    private Optional<ElasticsearchColumn> createColumn(String fieldPathType, List<String> arrays)
    {
        String[] items = fieldPathType.split(":");
        String path = items[0];
        Type prestoType;

        if (items.length != 2) {
            log.debug("Invalid column path format: " + fieldPathType);
            return Optional.empty();
        }

        if (!path.endsWith(".type")) {
            log.debug("Ignore column with no type info: " + fieldPathType);
            return Optional.empty();
        }

        String type = path.contains(".properties.") ? "nested" : items[1];

        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "long":
                prestoType = BIGINT;
                break;
            case "string":
            case "nested":
                prestoType = VARCHAR;
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "binary":
                prestoType = VARBINARY;
                break;
            default:
                prestoType = VARCHAR;
                break;
        }

        String propertyName = path.substring(0, path.indexOf('.'));
        return Optional.of(new ElasticsearchColumn(propertyName, prestoType, propertyName, type, arrays.contains(propertyName)));
    }
}
