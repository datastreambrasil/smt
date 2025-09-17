# Datastream SMT

Here you will find a list of Single Message Transforms (SMT) that can be used with Kafka Connect to transform data as it flows through the system.

## QlikToDebeziumDirectTransform
This transform is used to convert Qlik data into a format that can be directly ingested by Debezium. It is designed to work with the Qlik data model and transform it into a 
format that is compatible with Debezium's requirements.

How to use: 

```
    transforms: QlikToDebezium
    transforms.QlikToDebezium.type: br.com.datastreambrasil.kafka.connect.transforms.QlikToDebeziumDirectTransform
```

## AddFixedSchemaQlikToDebezium
This transform is used to add a fixed schema to Qlik data before it is ingested by Debezium. The schema is inferred from the message 
so you don't need to add any extra config. 

How to use:

```
    transforms: AddFixedSchemaQlikToDebezium
    transforms.AddFixedSchema.type: br.com.datastreambrasil.kafka.connect.transforms.AddFixedSchemaQlikToDebezium
```