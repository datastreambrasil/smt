package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AddFixedSchemaQlikToDebeziumTest {

    @Test
    public void testApplyInsert() {
        var qlikMessage = createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation.INSERT);
        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, null, qlikMessage);

        try (var transform = new AddFixedSchemaQlikToDebezium<SourceRecord>()) {
            var output = transform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertTrue(output.value() instanceof Map);
            Assert.assertNotNull(output.valueSchema());
            Assert.assertNotNull(output.valueSchema().field("data"));
        }
    }

    private Map<String,Object> createQlikAfterMessage(QlikToDebeziumDirectTransform.QlikOperation qlikOperation) {
        Map<String, Object> qlikMessage = new HashMap<>();
        qlikMessage.put("beforeData", null);
        qlikMessage.put("data", Map.of("IDT_EMP", 3));
        qlikMessage.put("headers", Map.of("operation", qlikOperation.toString()));
        return qlikMessage;
    }
}