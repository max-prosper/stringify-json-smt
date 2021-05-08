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

    private final Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
    private final Struct key = new Struct(keySchema).put("key", "key");

    private final StringifyJson<SinkRecord> xform = new StringifyJson.Value<>();

    private final String propTargetFields = "targetFields";

    private void runAssertions(Schema valueSchema, Object inputValue, String outputSchema, String outputValue) {
        final SinkRecord record = new SinkRecord("test", 0, this.keySchema, this.key, valueSchema, inputValue, 0);
        final SinkRecord output = xform.apply(record);
        final Struct inputStruct = (Struct) record.value();
        final Struct outputStruct = (Struct) output.value();

        Assertions.assertEquals(output.key(), record.key());
        Assertions.assertEquals(inputValue.toString(), inputStruct.toString());
        Assertions.assertEquals(outputValue, outputStruct.toString());
        Assertions.assertEquals(outputSchema, outputStruct.schema().fields().toString());
    }

    @Test
    public void integerField() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", Schema.INT32_SCHEMA)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", 42)
                .put("other_field", 24);

        final String outputValue = "Struct{target_field=42,other_field=24}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void booleanField() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", Schema.BOOLEAN_SCHEMA)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", true)
                .put("other_field", 24);

        final String outputValue = "Struct{target_field=true,other_field=24}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void objectField() {
        final Schema structSchema = SchemaBuilder.struct()
                .field("first", Schema.INT32_SCHEMA)
                .field("second", Schema.STRING_SCHEMA);
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", structSchema)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Object obj = new Struct(structSchema).put("first", 42).put("second", "2nd");
        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", obj)
                .put("other_field", 111);

        final String outputValue = "Struct{target_field={\"first\":42,\"second\":\"2nd\"},other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void mapField() {
        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", mapSchema)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Map<String, String> mapValue = new HashMap<>();
        mapValue.put("first", "1st");
        mapValue.put("second", "2nd");
        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", mapValue)
                .put("other_field", 111);

        final String outputValue = "Struct{target_field={\"first\":\"1st\",\"second\":\"2nd\"},other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void nestedMapField() {
        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        final Schema nestedMapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, mapSchema);
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", nestedMapSchema)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Map<String, Map<String, String>> nestedMapValue = new HashMap<>();
        final Map<String, String> mapValue = new HashMap<>();
        mapValue.put("first", "1st");
        mapValue.put("second", "2nd");
        nestedMapValue.put("first", mapValue);
        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", nestedMapValue)
                .put("other_field", 111);

        final String outputValue = "Struct{target_field={\"first\":{\"first\":\"1st\",\"second\":\"2nd\"}},other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void arrayOfIntsField() {
        final Schema arraySchema = SchemaBuilder.array(Schema.INT32_SCHEMA);
        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", arraySchema)
                .field("other_field", Schema.INT32_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final List<Integer> array = Arrays.asList(42, 24);
        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", array)
                .put("other_field", 256);

        final String outputValue = "Struct{target_field=[42, 24],other_field=256}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void arrayOfStructs() {
        final Schema structSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA);
        final Schema arraySchema = SchemaBuilder.array(structSchema).build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", arraySchema)
                .field("other_field", structSchema);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Struct obj1 = new Struct(structSchema).put("name", "Bob").put("age", 25);
        final Struct obj2 = new Struct(structSchema).put("name", "Alice");
        final List<Struct> array = Arrays.asList(obj1, obj2);

        final Struct inputValue = new Struct(valueSchema).put("target_field", array).put("other_field", obj2);

        final String outputValue = "Struct{target_field=[{\"name\":\"Bob\",\"age\":25}, {\"name\":\"Alice\"}],other_field=Struct{name=Alice}}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{STRUCT}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void arrayOfMaps() {
        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);
        final Schema arraySchema = SchemaBuilder.array(mapSchema).build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", arraySchema)
                .field("other_field", Schema.STRING_SCHEMA);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Map<Object, Object> map1 = new HashMap<>();
        map1.put("first", 1);
        map1.put("second", 2);
        final Map<Object, Object> map2 = new HashMap<>();
        map2.put("third", 3);
        final List<Map<Object, Object>> array = Arrays.asList(map1, map2);

        final Struct inputValue = new Struct(valueSchema).put("target_field", array).put("other_field", "other");

        final String outputValue = "Struct{target_field=[{\"first\":1,\"second\":2}, {\"third\":3}],other_field=other}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{STRING}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void nestedArrayOfStructs() {
        final Schema structSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA);
        final Schema arraySchema = SchemaBuilder.array(structSchema).build();
        final Schema nestedArraySchema = SchemaBuilder.array(arraySchema).build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", nestedArraySchema)
                .field("other_field", structSchema);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Struct obj1 = new Struct(structSchema).put("name", "Bob").put("age", 25);
        final Struct obj2 = new Struct(structSchema).put("name", "Alice");
        final List<Struct> array = Arrays.asList(obj1, obj2);
        final List<List<Struct>> nestedArray = Arrays.asList(array);

        final Struct inputValue = new Struct(valueSchema).put("target_field", nestedArray).put("other_field", obj2);

        final String outputValue = "Struct{target_field=[[{\"name\":\"Bob\",\"age\":25},{\"name\":\"Alice\"}]],other_field=Struct{name=Alice}}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{STRUCT}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void multipleTargetFields() {
        final Schema structSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA);
        final Schema arraySchema = SchemaBuilder.array(structSchema).build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field1", arraySchema)
                .field("target_field2", structSchema)
                .field("other_field", structSchema);

        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field1,target_field2");
        xform.configure(props);

        final Struct obj1 = new Struct(structSchema).put("name", "Bob").put("age", 25);
        final Struct obj2 = new Struct(structSchema).put("name", "Alice").put("age", 25);
        final Struct obj3 = new Struct(structSchema).put("name", "Jack");
        final List<Struct> array = Arrays.asList(obj1);

        final Struct inputValue = new Struct(valueSchema)
                .put("target_field1", array)
                .put("target_field2", obj2)
                .put("other_field", obj3);

        final String outputValue = "Struct{target_field1=[{\"name\":\"Bob\",\"age\":25}],target_field2={\"name\":\"Alice\",\"age\":25},other_field=Struct{name=Jack}}";
        final String outputSchema = "[Field{name=target_field1, index=0, schema=Schema{STRING}}, Field{name=target_field2, index=1, schema=Schema{STRING}}, Field{name=other_field, index=2, schema=Schema{STRUCT}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }
}
