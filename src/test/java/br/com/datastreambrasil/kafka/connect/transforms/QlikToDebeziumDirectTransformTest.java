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
    public void testApplyAfter() {

        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.INSERT);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, qlikMessage);

        try (var qlikTransform = new QlikToDebeziumDirectTransform<SourceRecord>()){
            var output = qlikTransform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            var outputValue = (Map<String, Object>) output.value();

            var payload = getMapFromMap(outputValue, "payload");
            Assert.assertNotNull(payload);
            Assert.assertEquals("c", payload.get("op"));

            var after = getMapFromMap(payload, "after");
            Assert.assertNotNull(after);
            Assert.assertEquals(2, after.get("IDT_EMP"));
        }

    }

    @Test
    public void testApplyBefore() {

        var qlikMessage = createQlikBeforeMessage(QlikToDebeziumDirectTransform.QlikOperation.DELETE);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, qlikMessage);

        try (var qlikTransform = new QlikToDebeziumDirectTransform<SourceRecord>()){
            var output = qlikTransform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            var outputValue = (Map<String, Object>) output.value();

            var payload = getMapFromMap(outputValue, "payload");
            Assert.assertNotNull(payload);
            Assert.assertEquals("d", payload.get("op"));

            var before = getMapFromMap(payload, "before");
            Assert.assertNotNull(before);
            Assert.assertEquals(2, before.get("IDT_EMP"));
        }

    }

    private Map<String,Object> getMapFromMap(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.get(key);
    }

    private Map<String,Object> createQlikBeforeMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", Map.of("IDT_EMP", 2));
        qlikMessage.put("data", null);
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }

    private Map<String,Object> createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", null);
        qlikMessage.put("data", Map.of("IDT_EMP", 2));
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }
}