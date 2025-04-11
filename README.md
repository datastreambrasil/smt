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