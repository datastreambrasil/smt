SchemaEvolutionTransform

*Descrição*

SchemaEvolutionTransform é uma transformação customizada para Kafka Connect, que recebe mensagens JSON no formato puro e as adapta para um formato compatível com conectores Debezium Sink JDBC (com Schema e Payload).

O objetivo é criar dinamicamente o schema e o payload a partir da mensagem recebida no formato esperado, permitindo a utilização correta do conector JDBC.

A entrada esperada é uma mensagem com a seguinte estrutura mínima:

{
  "data": { ... },
  "beforeData": { ... },
  "headers": {
    "operation": "INSERT"
  }
}

A transformação retorna uma mensagem no formato esperado pelo Debezium:

{
  "schema": {
    "type": "struct",
    "fields": [
      { "field": "IDT_CTA_CRT", "type": "int32" },
      { "field": "NOME", "type": "string" }
    ]
  },
  "payload": {
    "before": { ... },
    "after": { ... },
    "op": "c"
  }
}


*Configuração*

No conector Kafka Connect, incluir a transformação no config:

transforms: "SchemaEvolution",
transforms.SchemaEvolution.type: "br.com.datastreambrasil.kafka.connect.transforms.SchemaEvolutionTransform"

Exemplo de funcionamento (mensagem de entrada):

{
  "data": {
    "IDT_CTA_CRT": 539018171,
    "NOME": "João da Silva",
    "ATIVO": true
  },
  "beforeData": {
    "IDT_CTA_CRT": 539018171,
    "NOME": "João"
  },
  "headers": {
    "operation": "UPDATE"
  }
}


Exemplo de mensagem de saída transformada:

{
  "schema": {
    "type": "struct",
    "fields": [
      { "field": "IDT_CTA_CRT", "type": "int32" },
      { "field": "NOME", "type": "string" },
      { "field": "ATIVO", "type": "boolean" }
    ]
  },
  "payload": {
    "before": {
      "IDT_CTA_CRT": 539018171,
      "NOME": "João"
    },
    "after": {
      "IDT_CTA_CRT": 539018171,
      "NOME": "João da Silva",
      "ATIVO": true
    },
    "op": "u"
  }
}
