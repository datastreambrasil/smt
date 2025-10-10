package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.UUID;

public class GenerateGuid<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {

        String guid = UUID.randomUUID().toString();

        Schema newKeySchema = SchemaBuilder.string().build();

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                newKeySchema,
                guid,
                record.valueSchema(),
                record.value(),
                record.timestamp()
        );
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new org.apache.kafka.common.config.ConfigDef();
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }

    public static class Key<R extends ConnectRecord<R>> extends GenerateGuid<R> {}
}