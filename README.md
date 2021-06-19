# KafkaSnapshot
Tool that allows to read current data snapshot from Apache Kafka topic with compacting.

Current version could work with string and long keys and json messages and skips topics with NULL keys.

![Details](Details.PNG)

Config with topics for export

```yaml
"LoaderToolConfiguration": {
  "MetadataTimeout": 1000,
  "BootstrapServers": [
  ],
  "Topics": [
    {
      "Name": "test-string-topic",
      "KeyType": "String",
      "ExportFileName": "test_string_topic.json"
    },
    {
      "Name": "test-long-topic",
      "KeyType": "Long",
      "ExportFileName": "test_long_topic.json"
    }
  ]
}
```
