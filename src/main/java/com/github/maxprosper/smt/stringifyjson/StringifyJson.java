package com.github.maxprosper.smt.stringifyjson;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main class that implements stringify JSON transformation.
 */
@SuppressWarnings("unchecked")
abstract class StringifyJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyJson.class);

    private static final String PURPOSE = "StringifyJson SMT";
    private List<String> targetFields;

    private final String delimiterJoin = ".";

    interface ConfigName {
        String TARGET_FIELDS = "targetFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TARGET_FIELDS,
                    ConfigDef.Type.LIST, "",
                    ConfigDef.Importance.HIGH,
                    "Names of target fields. These fields will be stringified.");

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        targetFields = config.getList(ConfigName.TARGET_FIELDS);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            LOGGER.info("Schemaless records are not supported");
            return null;
        }

        Object recordValue = operatingValue(record);
        if (recordValue == null) {
            LOGGER.info("Record is null");
            LOGGER.info(record.toString());
            return record;
        }

        final Struct value = requireStruct(recordValue, PURPOSE);
        HashMap<String, String> stringifiedFields = stringifyFields(value, targetFields);

        if (stringifiedFields.size() == 0) {
            LOGGER.info("No target fields present in the record, nothing changed");
            LOGGER.info(record.toString());
            return record;
        }

        final Schema updatedSchema = makeUpdatedSchema(null, value, stringifiedFields);
        final Struct updatedValue = makeUpdatedValue(null, value, updatedSchema, stringifiedFields);

        return newRecord(record, updatedSchema, updatedValue);
    }

    /**
     * Stringify values from specified fields.
     *
     * @param value        Input record to read original fields.
     * @param targetFields List of fields to stringify values from.
     * @return Resulting stringified values by field names.
     */
    @SuppressWarnings("unchecked")
    private static HashMap<String, String> stringifyFields(Struct value, List<String> targetFields) {
        final HashMap<String, String> result = new HashMap<>(targetFields.size());
        final Schema valueSchema = value.schema();

        for (String field : targetFields) {
            String[] pathArr = field.split("\\.");
            List<String> path = Arrays.asList(pathArr);

            Object fieldSchema = getSchemaField(path, valueSchema);
            if (fieldSchema == null) {
                LOGGER.warn("target field {} is not present in the record schema {}", field, fieldSchema);
                continue;
            }

            Object fieldValue = getFieldValue(path, value);
            if (fieldValue == null) {
                result.put(field, "null");
                continue;
            }

            Schema fieldValueSchema = Values.inferSchema(fieldValue);
            if (fieldValueSchema == null) {
                result.put(field, fieldValue.toString());
                continue;
            }
            Schema.Type fieldValueType = fieldValueSchema.type();

            String strValue;
            if (fieldValueType.equals(Schema.Type.STRUCT)) {
                strValue = structToJSONObject((Struct) fieldValue).toString();

            } else if (fieldValueType.equals(Schema.Type.MAP)) {
                strValue = mapToJSONObject((HashMap) fieldValue).toString();

            } else if (fieldValueType.equals(Schema.Type.ARRAY)) {
                strValue = arrayValueToString((List<Object>) fieldValue);

            } else if (fieldValueType.equals(Schema.Type.STRING)) {
                strValue = fieldValue.toString();

            } else {
                strValue = String.valueOf(fieldValue);
            }

            result.put(field, strValue);
        }

        return result;
    }

    /**
     * Make schema for updated value.
     *
     * @param value             Input value to take the schema from.
     * @param stringifiedFields Resulting stringified values by field names.
     * @return New schema for output record.
     */
    private Schema makeUpdatedSchema(String parentKey, Struct value, HashMap<String, String> stringifiedFields) {
        if (value == null || value.schema() == null) {
            return null;
        }

        final SchemaBuilder builder = SchemaBuilder.struct();

        for (Field field : value.schema().fields()) {
            final Schema fieldSchema;
            final String absoluteKey = joinKeys(parentKey, field.name());

            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldSchema = field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;

            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldSchema = makeUpdatedSchema(absoluteKey, value.getStruct(field.name()), stringifiedFields);

            } else {
                fieldSchema = field.schema();
            }

            builder.field(field.name(), fieldSchema);
        }

        return builder.build();
    }

    /**
     * Replace values in target fields with stringified results and copy non-target values from original object.
     *
     * @param value             Original value.
     * @param updatedSchema     Schema for new output record.
     * @param stringifiedFields Stringified values by field names.
     * @return Output record with stringified values.
     */
    private Struct makeUpdatedValue(String parentKey, Struct value, Schema updatedSchema, HashMap<String, String> stringifiedFields) {
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            final Object fieldValue;
            final String absoluteKey = joinKeys(parentKey, field.name());

            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldValue = stringifiedFields.get(absoluteKey);
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldValue = makeUpdatedValue(absoluteKey, value.getStruct(field.name()),
                        updatedSchema.field(field.name()).schema(), stringifiedFields);
            } else {
                fieldValue = value.get(field.name());
            }

            updatedValue.put(field.name(), fieldValue);
        }

        return updatedValue;
    }

    @SuppressWarnings("unchecked")
    public static String arrayValueToString(List<Object> value) {
        if (value == null) {
            return "null";
        }
        if (value.size() == 0) {
            return "[]";
        }

        StringBuilder builder = new StringBuilder();

        for (Object elem : value) {
            if (builder.toString().length() != 0) {
                builder.append(", ");
            }
            if (elem == null) {
                builder.append("null");
                continue;
            }
            Schema valueSchema = Values.inferSchema(elem);
            if (valueSchema == null) {
                builder.append(elem.toString());
                continue;
            }
            Schema.Type valueType = valueSchema.type();
            if (valueType.equals(Schema.Type.STRUCT)) {
                builder.append(structToJSONObject((Struct) elem));

            } else if (valueType.equals(Schema.Type.MAP)) {
                builder.append(mapToJSONObject((HashMap) elem));

            } else if (valueType.equals(Schema.Type.ARRAY)) {
                builder.append(listToJSONArray((List<Object>) elem));

            } else if (valueType.equals(Schema.Type.STRING)) {
                builder.append("\"").append(elem).append("\"");

            } else {
                return value.toString();
            }
        }

        return "[" + builder.toString() + "]";
    }

    private static JSONObject structToJSONObject(Struct value) {
        JSONObject updatedObject = new JSONObject();

        String exceptionMsg = "Failed to put updated object value to field '{}', error: '{}'";
        for (Field field : value.schema().fields()) {
            Schema.Type fieldType = field.schema().type();
            if (fieldType.equals(Schema.Type.STRUCT)) {
                try {
                    updatedObject.put(field.name(), structToJSONObject(value.getStruct(field.name())));
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, field.name(), e.toString());
                    e.printStackTrace();
                    return null;
                }

            } else if (fieldType.equals(Schema.Type.ARRAY)) {
                try {
                    updatedObject.put(field.name(), listToJSONArray(value.getArray(field.name())));
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, field.name(), e.toString());
                    e.printStackTrace();
                    return null;
                }

            } else try {
                updatedObject.put(field.name(), value.get(field.name()));
            } catch (JSONException e) {
                LOGGER.error(exceptionMsg, field.name(), e.toString());
                e.printStackTrace();
                return null;
            }
        }

        return updatedObject;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject mapToJSONObject(HashMap value) {
        JSONObject updatedObject = new JSONObject();

        String exceptionMsg = "Failed to put updated map value for key '{}', error: '{}'";
        for (Object key : value.keySet()) {
            Object val = value.get(key);

            if (!(val instanceof Struct)) {
                try {
                    updatedObject.put(key.toString(), val);
                    continue;
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, key, e.toString());
                    e.printStackTrace();
                    return null;
                }
            }

            Struct structValue = (Struct) val;
            Schema.Type fieldType = structValue.schema().type();

            if (fieldType.equals(Schema.Type.STRUCT)) {
                try {
                    updatedObject.put(key.toString(), structToJSONObject(structValue));
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, key, e.toString());
                    e.printStackTrace();
                    return null;
                }

            } else if (fieldType.equals(Schema.Type.MAP)) {
                try {
                    updatedObject.put(key.toString(), mapToJSONObject((HashMap) val));
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, key, e.toString());
                    e.printStackTrace();
                    return null;
                }

            } else if (fieldType.equals(Schema.Type.ARRAY)) {
                try {
                    updatedObject.put(key.toString(), listToJSONArray((List<Object>) structValue));
                } catch (JSONException e) {
                    LOGGER.error(exceptionMsg, key, e.toString());
                    e.printStackTrace();
                    return null;
                }

            } else try {
                updatedObject.put(key.toString(), structValue);
            } catch (JSONException e) {
                LOGGER.error(exceptionMsg, key, e.toString());
                e.printStackTrace();
                return null;
            }
        }

        return updatedObject;
    }

    @SuppressWarnings("unchecked")
    private static JSONArray listToJSONArray(List<Object> value) {
        JSONArray result = new JSONArray();

        for (Object element : value) {
            if (!(element instanceof Struct)) {
                result.put(element);
                continue;
            }

            Struct struct = (Struct) element;
            if (struct.schema().type().equals(Schema.Type.ARRAY)) {
                result.put(listToJSONArray((List<Object>) element));
                continue;
            }
            if (struct.schema().type().equals(Schema.Type.MAP)) {
                result.put(mapToJSONObject((HashMap) element));
                continue;
            }

            result.put(structToJSONObject((Struct) element));
        }

        return result;
    }

    private static Object getSchemaField(List<String> path, Schema schema) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return schema.field(path.get(0));
        } else {
            return getSchemaField(path.subList(1, path.size()), schema.field(path.get(0)).schema());
        }
    }

    private static Object getFieldValue(List<String> path, Struct value) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return value.get(path.get(0));
        } else {
            return getFieldValue(path.subList(1, path.size()), value.getStruct(path.get(0)));
        }
    }

    private String joinKeys(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return parent + delimiterJoin + child;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends StringifyJson<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        }
    }
}
