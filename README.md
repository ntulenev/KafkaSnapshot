# KafkaSnapshot
Tool that allows to read current data snapshot from Apache Kafka topic with compacting.

Application supports Apache Kafka topics with string keys (optionally as json) and long keys. Topics with NULL keys will be skipped.
Topics could be filtered by key value (only for long keys in current version).

Messages should contains JSON data.

![Details](Details.PNG)

Config with topics for export:

```yaml
"LoaderToolConfiguration": {
  "MetadataTimeout": "00:00:05",
  "BootstrapServers": [
  ],
  "Topics": [
      {
        "Name": "topic1",
        "KeyType": "Json",
        "Compacting": "Off",
        "ExportFileName": "topic1.json"
      },
      {
        "Name": "topic2",
        "KeyType": "String",
        "Compacting": "On",
        "ExportFileName": "topic2.json"
      },
      {
        "Name": "topic3",
        "KeyType": "Long",
        "Compacting": "On",
        "ExportFileName": "topic3.json",
        "FilterType": "Equals",
        "FilterValue": "42"
      }
  ]
}
```

| Parameter name | Description   |
| -------------- | ------------- |
| MetadataTimeout | Cluster metadata loading timeout |
| BootstrapServers | List of kafka cluster servers, like "kafka-test:9092"  |
| Name           | Apache Kafka topic name |
| KeyType        | Apache Kafka topic key representation (Json,String,Long) |
| Compacting     | Use compacting by key or not (On,Off) |
| ExportFileName | File name for exported data  |
| FilterType | Equals or None (optional)  |
| FilterValue | String representation of filtering sample  |
