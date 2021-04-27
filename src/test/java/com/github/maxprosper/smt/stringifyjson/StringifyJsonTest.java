package com.github.maxprosper.smt.stringifyjson;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;
import org.junit.After;
import org.junit.jupiter.api.Assertions;

public class StringifyJsonTest {

    @After
    public void teardown() {
        xform.close();
    }

    private final StringifyJson<SinkRecord> xform = new StringifyJson.Value<>();
    private final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT8_SCHEMA);
    private final Schema ARRAY_SCHEMA = SchemaBuilder.array(STRUCT_SCHEMA).build();

    private final Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
    private final Struct key = new Struct(keySchema).put("key", "key");
    private final Schema valueSchema = SchemaBuilder.struct()
            .field("target_field", ARRAY_SCHEMA)
            .field("other_field", STRUCT_SCHEMA);

    private final String propSourceFields = "sourceFields";

    @Test
    public void basicCase() {
        Byte age = 25;
        Struct obj1 = new Struct(STRUCT_SCHEMA).put("name", "Bob").put("age", age);
        Struct obj2 = new Struct(STRUCT_SCHEMA).put("name", "Alice");
        List<Struct> array = Arrays.asList(obj1, obj2);

        final Map<String, String> props = new HashMap<>();
        props.put(propSourceFields, "target_field");
        xform.configure(props);

        final Struct value = new Struct(valueSchema).put("target_field", array).put("other_field", obj2);

        String outputValue = "Struct{target_field=[{\"name\":\"Bob\",\"age\":25}, {\"name\":\"Alice\"}],other_field=Struct{name=Alice}}";
        String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{STRUCT}}]";

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, valueSchema, value, 0);
        final SinkRecord output = xform.apply(record);

        final Struct inputStruct = (Struct) record.value();
        final Struct outputStruct = (Struct) output.value();

        Assertions.assertEquals(output.key(), record.key());
        Assertions.assertEquals(value.toString(), cleanSchemaString(inputStruct.toString()));
        Assertions.assertEquals(outputValue, cleanSchemaString(outputStruct.toString()));
        Assertions.assertEquals(outputSchema, cleanSchemaString(outputStruct.schema().fields().toString()));
    }

    String cleanSchemaString(final String schema) {
        return schema.replaceAll("@\\w+", "");
    }
}

