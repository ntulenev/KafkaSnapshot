# KafkaSnapshot
Tool that allows to read current data snapshot from Apache Kafka topic with compacting.

Application supports Apache Kafka topics with string keys (optionally as json) and long keys. Topics with NULL keys will be skipped.
Messages should contains JSON data.

![Details](Details.PNG)

Config with topics for export

```yaml
"LoaderToolConfiguration": {
  "MetadataTimeout": 1000,
  "BootstrapServers": [
  ],
  "Topics": [
      {
        "Name": "topic1",
        "KeyType": "Json",
        "ExportFileName": "topic1.json"
      },
      {
        "Name": "topic2",
        "KeyType": "String",
        "ExportFileName": "topic2.json"
      },
      {
        "Name": "topic3",
        "KeyType": "Long",
        "ExportFileName": "topic3.json"
      }
  ]
}
```
