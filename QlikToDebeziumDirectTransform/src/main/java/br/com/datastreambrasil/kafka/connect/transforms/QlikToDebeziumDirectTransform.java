package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class QlikToDebeziumDirectTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public enum QlikOperation {
        INSERT,
        UPDATE,
        DELETE
    }

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

        Map<String, Object> newMessage = new HashMap<>();

        newMessage.put("before", new HashMap<String, Object>());
        var hasBeforeData = false;
        if (qlikMessage.containsKey("beforeData")) {
            Map<String, Object> beforeData = (Map<String, Object>) qlikMessage.get("beforeData");
            if (beforeData != null) {
                newMessage.put("before", beforeData);
                hasBeforeData = true;
            }
        }

        newMessage.put("after", new HashMap<String, Object>());
        if (qlikMessage.containsKey("data") && qlikMessage.get("data") != null) {
            Map<String, Object> afterData = (Map<String, Object>) qlikMessage.get("data");
            if (afterData != null) {
                newMessage.put("after", afterData);
            }
        }

        String op = "c";
        if (qlikMessage.containsKey("headers")) {
            Map<String, Object> headers = (Map<String, Object>) qlikMessage.get("headers");
            if (headers != null && headers.containsKey("operation")) {
                Object operation = headers.get("operation");
                if (operation != null) {
                    if (QlikOperation.UPDATE.toString().equals(operation.toString())) {
                        op = "u";
                    }
                    if (QlikOperation.DELETE.toString().equals(operation.toString())) {
                        op = "d";

                        // qlik does not send the beforeData in the delete operation, so we set this using the after data
                        if (!hasBeforeData) {
                            newMessage.put("before", newMessage.get("after"));
                            newMessage.put("after", null);
                        }
                    }
                }
            }
        }
        newMessage.put("op", op);

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