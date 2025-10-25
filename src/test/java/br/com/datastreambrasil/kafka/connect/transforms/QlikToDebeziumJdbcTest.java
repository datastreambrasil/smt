package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QlikToDebeziumJdbcTest {

    @Test
    public void testApplyInsert() {
        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.INSERT);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, "\"48260\"+\"200\"", null, qlikMessage);

        try (var transform = new QlikToDebeziumJdbc<SourceRecord>()) {
            transform.primaryKeyFields = List.of("FIELD_A", "FIELD_B");
            var output = transform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.key() instanceof Struct);
            Struct keyStruct = (Struct) output.key();
            Assert.assertEquals("48260",keyStruct.get("FIELD_A"));
            Assert.assertEquals("200",keyStruct.get("FIELD_B"));
            Assert.assertTrue(output.value() instanceof Map);
            Assert.assertNotNull(output.valueSchema());
            Assert.assertNotNull(output.valueSchema().field("data"));
        }
    }

    @Test
    public void testApplyDelete() {
        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.DELETE);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, "\"48260\"+\"200\"", null, qlikMessage);

        try (var transform = new QlikToDebeziumJdbc<SourceRecord>()) {
            transform.primaryKeyFields = List.of("FIELD_A", "FIELD_B");
            var output = transform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.key() instanceof Struct);
            Struct keyStruct = (Struct) output.key();
            Assert.assertEquals("48260",keyStruct.get("FIELD_A"));
            Assert.assertEquals("200",keyStruct.get("FIELD_B"));
            Assert.assertNull(output.value());
            Assert.assertNull(output.valueSchema());
        }
    }

    private Map<String,Object> createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", null);
        qlikMessage.put("data", Map.of("FIELD_A", 48260, "FIELD_B", 200, "FIELD_C", "Some value"));
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }
}