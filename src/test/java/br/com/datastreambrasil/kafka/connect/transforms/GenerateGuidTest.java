package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.fail;

public class GenerateGuidTest {

    @Test
    public void testApplyGuidGeneration() {
        Map<String, Object> someMessage = new HashMap<>();
        someMessage.put("someField", "someValue");

        var source = new SourceRecord(null, null, "topic",
                Schema.STRING_SCHEMA, null, null, someMessage);

        try (var transform = new GenerateGuid<SourceRecord>()) {
            var output = transform.apply(source);
            Assert.assertNotNull(output);
            Assert.assertNull(output.valueSchema());
            Assert.assertThat(output.value(), instanceOf(Map.class));
            Assert.assertEquals("someValue", (String) ((Map) output.value()).get("someField"));

            Assert.assertNotNull(output.key());
            Assert.assertThat(output.key(), instanceOf(String.class));
            try {
                UUID.fromString((String) output.key());
            } catch (IllegalArgumentException e) {
                fail(output.key() + " is not a uuid");
            }

        }
    }

    @Test
    public void testConfigGuidGeneration() {
        try (var transform = new GenerateGuid<SourceRecord>()) {
            Assert.assertNotNull(transform.config());
        }

    }
}
