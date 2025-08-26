package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;

public class SchemaEvolutionTransform<R extends ConnectRecord<R>> implements Transformation<R> {

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

        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", "struct");

        List<Map<String, Object>> fields = new ArrayList<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            //"IDT_CTA_CRT": 539018171,
            //      { "field": "IDT_CTA_CRT", "type": "int32" },
            // {"field": "IDT_CTA_CRT", "type": "int32"}
            Map<String, Object> field = new HashMap<>();
            field.put("field", entry.getKey());

            Object value = entry.getValue();
            if (value instanceof Integer) {
                field.put("type", "int32");
            } else if (value instanceof Long) {
                field.put("type", "int64");
            } else if (value instanceof Boolean) {
                field.put("type", "boolean");
            } else if (value instanceof Double || value instanceof Float) {
                field.put("type", "double");
            } else {
                // default -> string (inclusive nulls e datas)
                field.put("type", "string");
            }

            fields.add(field);
        }

        schemaMap.put("fields", fields);

        Map<String, Object> payload = new HashMap<>();
        payload.put("after", data);
        payload.put("before", beforeData);
        payload.put("op", headers.get("operation").toString().substring(0,1).toLowerCase());

        Map<String, Object> newMessage = new HashMap<>();
        newMessage.put("schema", schemaMap);
        newMessage.put("payload", payload);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
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
