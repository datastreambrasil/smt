package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;

public class QlikToDebeziumJdbc<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String CFG_PRIMARY_KEY_FIELDS = "primary.key.fields";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CFG_PRIMARY_KEY_FIELDS, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH,
                    "List of primary key fields, separated by comma.");

    protected List<String> primaryKeyFields;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);

        // init configs
        primaryKeyFields = config.getList(CFG_PRIMARY_KEY_FIELDS);
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

        Schema schemaValue = SchemaBuilder.struct().field("data", SchemaBuilder.struct().build())
                .name("QlikDebezium.CloudEvents.Envelope")
                .build();
        Object value = record.value();

        if (value instanceof Map) {
            Map<String,Object> valueMap = (Map<String,Object>) value;
            Map<String,Object> headersMap = (Map<String,Object>) valueMap.get("headers");
            String op = String.valueOf(headersMap.get("operation"));
            if ("delete".equalsIgnoreCase(op)){
                // we set null, so JDBC Debezium understands it's a delete operation
                value = null;
                schemaValue = null;
            }
        }

        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        for (String pkField : primaryKeyFields) {
            keySchemaBuilder.field(pkField, SchemaBuilder.string().build());
        }
        Schema keySchema = keySchemaBuilder.build();
        Struct keyStruct = new Struct(keySchema);
        String[] keys = record.key().toString().split("\\+");
        for (int i = 0; i < keys.length; i++) {
            keyStruct.put(primaryKeyFields.get(i), keys[i].replace("\"", ""));
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                keySchema,
                keyStruct,
                schemaValue,
                value,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

}