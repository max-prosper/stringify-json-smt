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
import org.apache.kafka.connect.errors.DataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

abstract class StringifyJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyJson.class);

    private static final String PURPOSE = "StringifyJson expansion";
    private List<String> sourceFields;

    private String delimiterJoin = ".";

    interface ConfigName {
        String SOURCE_FIELDS = "sourceFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELDS,
                    ConfigDef.Type.LIST, "",
                    ConfigDef.Importance.HIGH,
                    "Source field name. This field will be expanded to json object.");

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFields = config.getList(ConfigName.SOURCE_FIELDS);
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            LOGGER.info("Schemaless records are not supported");
            return null;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        try {
            Object recordValue = record.value();
            if (recordValue == null) {
                LOGGER.info("Record is null");
                LOGGER.info(record.toString());
                return record;
            }

            final Struct value = requireStruct(recordValue, PURPOSE);
            HashMap<String, String> stringifiedFields = stringifyFields(value, sourceFields);

            final Schema updatedSchema = makeUpdatedSchema(null, value, stringifiedFields);
            final Struct updatedValue = makeUpdatedValue(null, value, updatedSchema, stringifiedFields);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        } catch (DataException e) {
            LOGGER.info("StringifyJson fields missing from record: " + record.toString(), e);
            return record;
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

    @SuppressWarnings("unchecked")
    public static String arrayObjectToString(Object value) {
        if (value == null) {
            return "[]";
        }
        List<Object> inputArr = (List<Object>) value;
        StringBuilder builder = new StringBuilder();

        for (Object elem : inputArr) {
            if (builder.toString().length() != 0) {
                builder.append(", ");
            }
            Schema.Type valueType = Values.inferSchema(elem).type();
            if (valueType == Schema.Type.STRUCT || valueType == Schema.Type.MAP) {
                builder.append(toJSONObject(elem));

            } else if (valueType == Schema.Type.ARRAY) {
                builder.append(toJSONArray((List<Object>) elem));

            } else if (valueType == Schema.Type.STRING) {
                builder.append("\"").append(elem).append("\"");

            } else {
                return inputArr.toString();
            }
        }

        return "[" + builder.toString() + "]";
    }

    private static HashMap<String, String> stringifyFields(Struct value, List<String> sourceFields) {
        final HashMap<String, String> result = new HashMap<>(sourceFields.size());

        for (String field : sourceFields) {
            String[] pathArr = field.split("\\.");
            List<String> path = Arrays.asList(pathArr);
            Object fieldValue = getFieldValue(path, value);

            String strValue;
            Schema.Type fieldValueType = Values.inferSchema(fieldValue).type();

            if (fieldValueType == Schema.Type.STRUCT || fieldValueType == Schema.Type.MAP) {
                strValue = toJSONObject(fieldValue).toString();

            } else if (fieldValueType == Schema.Type.STRING) {
                strValue = fieldValue.toString();

            } else if (fieldValueType == Schema.Type.ARRAY) {
                strValue = arrayObjectToString(fieldValue);

            } else {
                strValue = String.valueOf(fieldValue);
            }
            result.put(field, strValue);
        }

        return result;
    }

    private String joinKeys(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return parent + delimiterJoin + child;
    }

    private Schema makeUpdatedSchema(String parentKey, Struct value, HashMap<String, String> stringifiedFields) {
        final SchemaBuilder builder = SchemaBuilder.struct();

        for (Field field : value.schema().fields()) {
            final Schema fieldSchema;
            final String absoluteKey = joinKeys(parentKey, field.name());

            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldSchema = field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            } else if (field.schema().type() == Schema.Type.STRUCT) {
                fieldSchema = makeUpdatedSchema(absoluteKey, value.getStruct(field.name()), stringifiedFields);
            } else {
                fieldSchema = field.schema();
            }

            builder.field(field.name(), fieldSchema);
        }

        return builder.build();
    }

    private Struct makeUpdatedValue(String parentKey, Struct value, Schema updatedSchema, HashMap<String, String> stringifiedFields) {
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            final Object fieldValue;
            final String absoluteKey = joinKeys(parentKey, field.name());
            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldValue = stringifiedFields.get(absoluteKey);
            } else if (field.schema().type() == Schema.Type.STRUCT) {
                fieldValue = makeUpdatedValue(absoluteKey, value.getStruct(field.name()),
                        updatedSchema.field(field.name()).schema(), stringifiedFields);
            } else {
                fieldValue = value.get(field.name());
            }
            updatedValue.put(field.name(), fieldValue);
        }

        return updatedValue;
    }

    private static JSONObject toJSONObject(Object obj) {
        JSONObject updatedObject = new JSONObject();
        Struct struct = (Struct) obj;
        Schema schema = struct.schema();

        String exceptionMsg = "Failed to put updated object value to field '{}', erorr: '{}'";
        for (Field field : schema.fields()) {
            Schema.Type fieldType = field.schema().type();
            if (fieldType == Schema.Type.STRUCT) {
                Struct fieldValue = struct.getStruct(field.name());
                try {
                    updatedObject.put(field.name(), toJSONObject(fieldValue));
                } catch (JSONException e) {
                    LOGGER.info(exceptionMsg, field.name(), e.toString());
                    e.printStackTrace();
                }

            } else if (fieldType == Schema.Type.ARRAY) {
                try {
                    updatedObject.put(field.name(), toJSONArray(struct.getArray(field.name())));
                } catch (JSONException e) {
                    LOGGER.info(exceptionMsg, field.name(), e.toString());
                    e.printStackTrace();
                }

            } else try {
                updatedObject.put(field.name(), struct.get(field.name()));
            } catch (JSONException e) {
                LOGGER.info(exceptionMsg, field.name(), e.toString());
                e.printStackTrace();
            }
        }

        return updatedObject;
    }

    @SuppressWarnings("unchecked")
    private static JSONArray toJSONArray(List<Object> array) {
        JSONArray result = new JSONArray();

        for (Object arrayElement : array) {
            if (!(arrayElement instanceof Struct)) {
                result.put(arrayElement);
                continue;
            }

            Struct struct = (Struct) arrayElement;
            if (struct.schema().type() == Schema.Type.ARRAY) {
                result.put(toJSONArray((List<Object>) arrayElement));
                continue;
            }

            result.put(toJSONObject(arrayElement));
        }

        return result;
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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}

