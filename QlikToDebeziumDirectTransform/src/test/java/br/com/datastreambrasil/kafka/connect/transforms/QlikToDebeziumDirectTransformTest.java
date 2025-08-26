package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class QlikToDebeziumDirectTransformTest {

    @Test
    public void testApplyInsert() {

        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.INSERT);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, qlikMessage);

        try (var qlikTransform = new QlikToDebeziumDirectTransform<SourceRecord>()){
            var output = qlikTransform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            var outputValue = (Map<String, Object>) output.value();

            Assert.assertEquals("c", outputValue.get("op"));
            var after = getMapFromMap(outputValue, "after");
            Assert.assertNotNull(after);
            Assert.assertEquals(3, after.get("IDT_EMP"));
        }

    }

    @Test
    public void testApplyDelete() {

        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.DELETE);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, qlikMessage);

        try (var qlikTransform = new QlikToDebeziumDirectTransform<SourceRecord>()){
            var output = qlikTransform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            var outputValue = (Map<String, Object>) output.value();

            Assert.assertEquals("d", outputValue.get("op"));

            var before = getMapFromMap(outputValue, "before");
            Assert.assertNotNull(before);
            Assert.assertEquals(3, before.get("IDT_EMP"));
        }

    }

    @Test
    public void testApplyUpdate() {

        var qlikMessage = createQlikBeforeAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.UPDATE);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, qlikMessage);

        try (var qlikTransform = new QlikToDebeziumDirectTransform<SourceRecord>()){
            var output = qlikTransform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            var outputValue = (Map<String, Object>) output.value();

            Assert.assertEquals("u", outputValue.get("op"));

            var before = getMapFromMap(outputValue, "before");
            Assert.assertNotNull(before);
            Assert.assertEquals(2, before.get("IDT_EMP"));

            var after = getMapFromMap(outputValue, "after");
            Assert.assertNotNull(after);
            Assert.assertEquals(3, after.get("IDT_EMP"));
        }

    }

    private Map<String,Object> getMapFromMap(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.get(key);
    }

    private Map<String,Object> createQlikBeforeAfterMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", Map.of("IDT_EMP", 2));
        qlikMessage.put("data", Map.of("IDT_EMP", 3));
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }

    private Map<String,Object> createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", null);
        qlikMessage.put("data", Map.of("IDT_EMP", 3));
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }
}