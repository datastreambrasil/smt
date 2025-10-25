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

## QlikToDebeziumJdbc
This transform is used to convert Qlik data into a format that can be ingested by Debezium when using JDBC connectors. 
It transforms the Qlik data model into a format that is compatible with Debezium's JDBC requirements.

How to use:

```
    transforms: QlikToDebeziumJdbc
    transforms.QlikToDebeziumJdbc.type: br.com.datastreambrasil.kafka.connect.transforms.QlikToDebeziumJdbc
    transforms.QlikToDebeziumJdbc.primary.key.fields: "IDT_ACA_CMC,IDT_TIP_ACA_ATM"
```

Important to note that the property `primary.key.fields` should follow the same order as the key received in kafka topic.

## GenerateKey
This transform is used to generate a GUID (Globally Unique Identifier) for a record key.

How to use:

```
    transforms: GenerateKey
    transforms.GenerateKey.type: br.com.datastreambrasil.kafka.connect.transforms.GenerateGuid$Key
```