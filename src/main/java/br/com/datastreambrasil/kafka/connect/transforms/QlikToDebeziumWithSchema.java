package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;

public class QlikToDebeziumWithSchema<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (!(record.value() instanceof Map)) {
            throw new RuntimeException("Input message is not in expected Map format");
        }

        Map<String, Object> qlikMessage = (Map<String, Object>) record.value();

        if (!qlikMessage.containsKey("data")) {
            return record;
        }

        Map<String, Object> data = (Map<String, Object>) qlikMessage.get("data");
        Map<String, Object> beforeData = (Map<String, Object>) qlikMessage.get("beforeData");
        Map<String, Object> headers = (Map<String, Object>) qlikMessage.get("headers");

        // --- 1. Construir schema dinâmico ---
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("QlikRecord");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Integer) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
            } else if (value instanceof Long) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
            } else if (value instanceof Boolean) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (value instanceof Double || value instanceof Float) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else {
                // default -> string (para nulls, datas, etc)
                schemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }

        Schema schema = schemaBuilder.build();


        Map<String, Object> payload = new HashMap<>();
        payload.put("after", data);
        payload.put("before", beforeData);
        payload.put("op", headers.get("operation").toString().substring(0,1).toLowerCase());

        Map<String, Object> newMessage = new HashMap<>();
        newMessage.put("payload", payload);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                schema,
                newMessage,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }
}
