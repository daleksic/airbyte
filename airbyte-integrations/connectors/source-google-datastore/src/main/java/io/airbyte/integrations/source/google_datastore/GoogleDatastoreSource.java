/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.integrations.source.google_datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.ProjectionEntity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.NoOpJdbcStreamingQueryConfiguration;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import io.airbyte.protocol.models.SyncMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.type.UnknownTypeException;

import static java.util.Objects.isNull;

public class GoogleDatastoreSource extends BaseConnector implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleDatastoreSource.class);

    static final String CONFIG_PROJECT_ID = "project_id";
    static final String CONFIG_NAMESPACE = "namespace";
    static final String CONFIG_CREDS = "credentials_json";


      // TODO insert your driver name. Ex: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      static final String DRIVER_CLASS = "driver_name_here";

      public GoogleDatastoreSource() {
        // By default NoOpJdbcStreamingQueryConfiguration class is used, but may be updated. See see example
        // MssqlJdbcStreamingQueryConfiguration
        //super(DRIVER_CLASS, new NoOpJdbcStreamingQueryConfiguration());
      }
/*
      // TODO The config is based on spec.json, update according to your DB
      @Override
      public JsonNode toJdbcConfig(JsonNode aqqConfig) {
        // TODO create DB config. Ex: "Jsons.jsonNode(ImmutableMap.builder().put("username",
        // userName).put("password", pas)...build());
        return null;
      }

      @Override
      public Set<String> getExcludedInternalSchemas() {
        // TODO Add tables to exaclude, Ex "INFORMATION_SCHEMA", "sys", "spt_fallback_db", etc
        return Set.of("");
      }
    */


    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        final String namespace = config.get(CONFIG_NAMESPACE).asText();
        Datastore datastore = getDatastore(config);
        List<AirbyteStream> streams = new ArrayList<>();

        List<String> kinds = getKinds(datastore);

        kinds.forEach(kind -> {
            Map<String, List<Field>> kindFields = getKindFields(kind, datastore);
            streams.add(CatalogHelpers.createAirbyteStream(kind, namespace, kindFields.get(kind))
                    .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)));
                    //.withSourceDefinedPrimaryKey(Types.boxToListofList(tableInfo.getPrimaryKeys()))) primary key BQ?
        });

        return new AirbyteCatalog().withStreams(streams);
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {
        final AirbyteConnectionStatus check = check(config);

        if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
            throw new RuntimeException("Unable establish a connection: " + check.getMessage());
        }
        final Instant emittedAt = Instant.now();
        Datastore datastore = getDatastore(config);

        final List<AutoCloseableIterator<AirbyteMessage>> incrementalIterators = getIncrementalIterators(datastore, catalog, state, emittedAt);;
        final List<AutoCloseableIterator<AirbyteMessage>> fullRefreshIterators = getFullRefreshIterators(datastore, catalog, state, emittedAt);

        final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = Stream.of(incrementalIterators, fullRefreshIterators)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        return AutoCloseableIterators.concatWithEagerClose(iteratorList);
    }

    public List<AutoCloseableIterator<AirbyteMessage>> getIncrementalIterators(Datastore datastore, ConfiguredAirbyteCatalog catalog, JsonNode state, Instant emittedAt) {
        return getSelectedIterators(
                datastore,
                catalog,
                state,
                emittedAt,
                configuredStream -> configuredStream.getSyncMode().equals(SyncMode.INCREMENTAL));
    }

    private List<AutoCloseableIterator<AirbyteMessage>> getFullRefreshIterators(Datastore datastore, ConfiguredAirbyteCatalog catalog, JsonNode state, Instant emittedAt) {
        return getSelectedIterators(
                datastore,
                catalog,
                state,
                emittedAt,
                configuredStream -> configuredStream.getSyncMode().equals(SyncMode.FULL_REFRESH));
    }

    private List<AutoCloseableIterator<AirbyteMessage>> getSelectedIterators(Datastore datastore, ConfiguredAirbyteCatalog catalog, JsonNode state, Instant emittedAt, Predicate<ConfiguredAirbyteStream> selector) {
        final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = new ArrayList<>();

        for (final ConfiguredAirbyteStream airbyteStream : catalog.getStreams()) {
            if (selector.test(airbyteStream)) {
                // missing check if stream is from current source
                final AutoCloseableIterator<AirbyteMessage> tableReadIterator = createReadIterator(datastore, airbyteStream, state, emittedAt);
                iteratorList.add(tableReadIterator);
            }
        }
        return iteratorList;
    }

    private AutoCloseableIterator<AirbyteMessage> createReadIterator(Datastore datastore, ConfiguredAirbyteStream airbyteStream, JsonNode state, Instant emittedAt) {

        final AutoCloseableIterator<AirbyteMessage> iterator;

        final String kindName = airbyteStream.getStream().getName();
        final String namespace = airbyteStream.getStream().getNamespace();
        final Set<String> selectedFieldsInCatalog = CatalogHelpers.getTopLevelFieldNames(airbyteStream);

        if (airbyteStream.getSyncMode() == SyncMode.INCREMENTAL) {
            //incremental mode
            final String cursorField = getCursorField(airbyteStream);
            final AutoCloseableIterator<AirbyteMessage> airbyteMessageIterator;

            if (state != null) {
                airbyteMessageIterator = getIncrementalStream(datastore, kindName, namespace, selectedFieldsInCatalog, state, cursorField, emittedAt);
            } else {
                airbyteMessageIterator = getFullRefreshStream(datastore, kindName, namespace, selectedFieldsInCatalog, emittedAt);
            }
            final JsonSchemaPrimitive cursorType = getCursorType(airbyteStream, cursorField);

            //iterator = AutoCloseableIterators.transform(autoCloseableIterator -> new StateDecoratingIterator(autoCloseableIterator, stateManager, pair, cursorField, cursorOptional.orElse(null), cursorType), airbyteMessageIterator);
            iterator= null;
        } else if (airbyteStream.getSyncMode() == SyncMode.FULL_REFRESH) {
            iterator = getFullRefreshStream(datastore, kindName, namespace, selectedFieldsInCatalog, emittedAt);
        } else if (airbyteStream.getSyncMode() == null) {
            throw new IllegalArgumentException(String.format("%s requires a source sync mode", GoogleDatastoreSource.class));
        } else {
            throw new IllegalArgumentException(String.format("%s does not support sync mode: %s.", GoogleDatastoreSource.class, airbyteStream.getSyncMode()));
        }

        final AtomicLong recordCount = new AtomicLong();
        return AutoCloseableIterators.transform(iterator, r -> {
            final long count = recordCount.incrementAndGet();
            if (count % 10000 == 0) {
                LOGGER.info("Reading kind {}. Records read: {}", kindName, count);
            }
            return r;
        });
    }

    private AutoCloseableIterator<AirbyteMessage> getIncrementalStream(Datastore datastore, String kindName, String namespace, Set<String> selectedDatabaseFields, JsonNode state, String cursorFieldName, Instant emittedAt) {
        //final String streamName = airbyteStream.getStream().getName();
        //final String namespace = airbyteStream.getStream().getNamespace();
        //final String cursorField = IncrementalUtils.getCursorField(airbyteStream);

        final List<Field> kindFields = getKindFields(kindName, datastore).get(kindName);

        /*final JDBCType cursorJdbcType = table.getFields().stream()
                .filter(info -> info.getColumnName().equals(cursorField))
                .map(ColumnInfo::getColumnType)
                .findFirst()
                .orElseThrow();
*/
        Preconditions.checkState(kindFields.stream().anyMatch(field -> field.getName().equals(cursorFieldName)),
                String.format("Could not find cursor field %s in kind %s", cursorFieldName, kindName));

        final AutoCloseableIterator<JsonNode> queryIterator = queryTableIncremental(datastore, state, selectedDatabaseFields, namespace, kindName, cursorFieldName);

        return getMessageIterator(queryIterator, kindName, namespace, emittedAt.toEpochMilli());
    }

    private AutoCloseableIterator<AirbyteMessage> getFullRefreshStream(Datastore datastore, String kindName, String namespace, Set<String> selectedDatabaseFields, Instant emittedAt) {

        final AutoCloseableIterator<JsonNode> queryIterator = queryKindFullRefresh(datastore, selectedDatabaseFields, namespace, kindName);

        return getMessageIterator(queryIterator, kindName, namespace, emittedAt.toEpochMilli());
    }

    public AutoCloseableIterator<JsonNode> queryTableIncremental(Datastore datastore, JsonNode state, Set<String> columnNames, String namespace, String kindName, String cursorFieldName) {

        LOGGER.info("Queueing query for kind: {}", kindName);
        LOGGER.info("Cursor : {}", cursorFieldName);
        LOGGER.info("State : {}", state);
        return AutoCloseableIterators.lazyIterator(() -> {
            try {

                List<JsonNode> result = new ArrayList<>();
                String[] projectionFields = columnNames.toArray(new String[columnNames.size()]);
                String projection = projectionFields[0];
                String[] projections = Arrays.copyOfRange(projectionFields, 1, columnNames.size());

                Query<ProjectionEntity> query = Query.newProjectionEntityQueryBuilder()
                        .setKind(kindName)
                        //.setFilter(StructuredQuery.PropertyFilter.gt(cursorFieldName, ))
                        .setProjection(projection, projections)
                        .build();

                QueryResults<ProjectionEntity> queryResult = datastore.run(query);
                while (queryResult.hasNext()) {
                    ProjectionEntity entity = queryResult.next();
                    Map<String, Value<?>> properties = entity.getProperties();

                    final ObjectNode node = (ObjectNode)Jsons.jsonNode(Collections.emptyMap());
                    properties.keySet().forEach(key -> {
                        addFieldValue(node, entity, key, properties.get(key).getType());
                    });
                    result.add(node);
                }

                return AutoCloseableIterators.fromStream(result.stream());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    public AutoCloseableIterator<JsonNode> queryKindFullRefresh(Datastore datastore, Set<String> columnNames, String namespace, String kindName) {
        LOGGER.info("Queueing query for kind: {}", kindName);
        return AutoCloseableIterators.lazyIterator(() -> {
            try {
                List<JsonNode> result = new ArrayList<>();
                String[] projectionFields = columnNames.toArray(new String[columnNames.size()]);
                String projection = projectionFields[0];
                String[] projections = Arrays.copyOfRange(projectionFields, 1, columnNames.size());

                Query<ProjectionEntity> query = Query.newProjectionEntityQueryBuilder()
                        .setKind(kindName)
                        .setProjection(projection, projections)
                        .build();

                QueryResults<ProjectionEntity> queryResult = datastore.run(query);
                while (queryResult.hasNext()) {
                    ProjectionEntity entity = queryResult.next();
                    Map<String, Value<?>> properties = entity.getProperties();

                    final ObjectNode node = (ObjectNode)Jsons.jsonNode(Collections.emptyMap());
                    properties.keySet().forEach(key -> {
                        addFieldValue(node, entity, key, properties.get(key).getType());
                    });
                    result.add(node);
                }
                return AutoCloseableIterators.fromStream(result.stream());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void addFieldValue(ObjectNode node, ProjectionEntity entity, String fieldName, ValueType fieldType) {
        switch(fieldType) {
            case STRING:
            case RAW_VALUE:
            case NULL:
                node.put(fieldName, entity.getString(fieldName));
                break;
            case LONG:
                node.put(fieldName, entity.getLong(fieldName));
                break;
            case DOUBLE:
                node.put(fieldName, entity.getDouble(fieldName));
                break;
            case BOOLEAN:
                node.put(fieldName, entity.getBoolean(fieldName));
                break;
            case TIMESTAMP:
                node.put(fieldName, entity.getTimestamp(fieldName).toString());
                break;
            case KEY:
                node.put(fieldName, entity.getKey(fieldName).toString());
                break;
            case ENTITY:
                node.put(fieldName, entity.getEntity(fieldName).toString());
                break;
            case LIST:
                node.put(fieldName, entity.getList(fieldName).toString());
                break;
            case LAT_LNG:
                node.put(fieldName, entity.getLatLng(fieldName).toString());
                break;
        }
    }

    public static AutoCloseableIterator<AirbyteMessage> getMessageIterator(AutoCloseableIterator<JsonNode> recordIterator,
                                                                           String streamName,
                                                                           String namespace,
                                                                           long emittedAt) {
        return AutoCloseableIterators.transform(recordIterator, r -> new AirbyteMessage()
                .withType(AirbyteMessage.Type.RECORD)
                .withRecord(new AirbyteRecordMessage()
                        .withStream(streamName)
                        .withNamespace(namespace)
                        .withEmittedAt(emittedAt)
                        .withData(r)));
    }

    public static String getCursorField(ConfiguredAirbyteStream stream) {
        if (stream.getCursorField().size() == 0) {
            throw new IllegalStateException("No cursor field specified for stream attempting to do incremental.");
        } else if (stream.getCursorField().size() > 1) {
            throw new IllegalStateException("Datastore does not support nested cursor fields.");
        } else {
            return stream.getCursorField().get(0);
        }
    }

    public static JsonSchemaPrimitive getCursorType(ConfiguredAirbyteStream stream, String cursorField) {
        if (stream.getStream().getJsonSchema().get("properties") == null) {
            throw new IllegalStateException(String.format("No properties found in stream: %s.", stream.getStream().getName()));
        }

        if (stream.getStream().getJsonSchema().get("properties").get(cursorField) == null) {
            throw new IllegalStateException(
                    String.format("Could not find cursor field: %s in schema for stream: %s.", cursorField, stream.getStream().getName()));
        }

        if (stream.getStream().getJsonSchema().get("properties").get(cursorField).get("type") == null) {
            throw new IllegalStateException(
                    String.format("Could not find cursor type for field: %s in schema for stream: %s.", cursorField, stream.getStream().getName()));
        }

        return JsonSchemaPrimitive.valueOf(stream.getStream().getJsonSchema().get("properties").get(cursorField).get("type").asText().toUpperCase());
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) {
        //try {
        Datastore datastore = getDatastore(config);

        List<String> kinds = getKinds(datastore);

        if (kinds != null && !kinds.isEmpty()) {
            return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
        }

        return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("Datastore check failed.");

        //  } catch (Exception e) {
        //   LOGGER.info("Datastore check failed.", e);
        //    return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage(e.getMessage() != null ? e.getMessage() : e.toString());
        // }
    }

    public static boolean isUsingJsonCredentials(JsonNode config) {
        return config.has(CONFIG_CREDS) && !config.get(CONFIG_CREDS).asText().isEmpty();
    }

    private Datastore getDatastore(JsonNode config) {
        final String projectId = config.get(CONFIG_PROJECT_ID).asText();
        final String namespace = config.get(CONFIG_NAMESPACE).asText();

        try {
            ServiceAccountCredentials credentials = null;

            if (isUsingJsonCredentials(config)) {
                // handle the credentials json being passed as a json object or a json object already serialized as
                // a string.
                final String credentialsString =
                        config.get(CONFIG_CREDS).isObject() ? Jsons.serialize(config.get(CONFIG_CREDS)) : config.get(CONFIG_CREDS).asText();
                credentials = ServiceAccountCredentials
                        .fromStream(new ByteArrayInputStream(credentialsString.getBytes(Charsets.UTF_8)));
            }

            return DatastoreOptions.newBuilder()
                    .setProjectId(projectId)
                    .setNamespace(System.getenv(namespace))
                    .setCredentials(!isNull(credentials) ? credentials : ServiceAccountCredentials.getApplicationDefault())
                    .build().getService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getKinds(Datastore datastore) throws DatastoreException {
        Query<Key> query = Query.newKeyQueryBuilder().setKind("__kind__").build();
        List<String> kinds = new ArrayList<>();
        QueryResults<Key> results = datastore.run(query);
        while (results.hasNext()) {
            kinds.add(results.next().getName());
        }

        return kinds;
    }

    private Map<String, List<Field>> getKindFields(String kind, Datastore datastore) {
        Map<String, List<Field>> kindFields = new HashMap<>();
        List<Field> fields = new ArrayList<>();

        Key key = datastore.newKeyFactory().setKind("__kind__").newKey(kind);
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("__property__")
                .setFilter(StructuredQuery.PropertyFilter.hasAncestor(key))
                .build();

        QueryResults<Entity> results = datastore.run(query);

        Map<String, Collection<String>> representationsByProperty = new HashMap<>();
        while (results.hasNext()) {
            Entity result = results.next();
            String propertyName = result.getKey().getName();
            List<StringValue> representations = result.getList("property_representation");
            Collection<String> currentRepresentations = representationsByProperty.get(propertyName);
            if (currentRepresentations == null) {
                currentRepresentations = new HashSet<>();
                representationsByProperty.put(propertyName, currentRepresentations);
            }
            for (StringValue value : representations) {
                currentRepresentations.add(value.get());
            }

            fields.add(Field.of(propertyName, getJsonSchemaPrimitiveType(getFieldsType(representationsByProperty.get(propertyName)))));
        }
        kindFields.put(kind, fields);
        return kindFields;
    }

    private JsonSchemaPrimitive getJsonSchemaPrimitiveType(String type) {
        switch (type) {
            case "INT64":
            case "DOUBLE":
                return JsonSchemaPrimitive.NUMBER;
            case "BOOLEAN":
                return JsonSchemaPrimitive.BOOLEAN;
            case "STRING":
                return JsonSchemaPrimitive.STRING;
            case "POINT":
            case "REFERENCE":
                return JsonSchemaPrimitive.OBJECT;
            case "NULL":
                return JsonSchemaPrimitive.NULL;
            default:
                LOGGER.warn("Unknown kind field type: {}", type);
                throw new RuntimeException("Unknown kind field type: " + type);
        }
    }

    private String getFieldsType(Collection<String> representations) {
        if (representations.size() > 1 && representations.contains("NULL")) {
            representations.remove("NULL");
            return new ArrayList<>(representations).get(0);

        }
        return new ArrayList<>(representations).get(0);
    }

    public static void main(String[] args) throws Exception {
        final Source source = new GoogleDatastoreSource();
        LOGGER.info("starting source: {}", GoogleDatastoreSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("completed source: {}", GoogleDatastoreSource.class);
    }
}
