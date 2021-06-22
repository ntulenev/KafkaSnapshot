# KafkaSnapshot
Tool that allows to read current data snapshot from Apache Kafka topic with compacting.

Current version could work with json and long keys and json messages and skips topics with NULL keys.

![Details](Details.PNG)

Config with topics for export

```yaml
"LoaderToolConfiguration": {
  "MetadataTimeout": 1000,
  "BootstrapServers": [
  ],
  "Topics": [
    {
      "Name": "test-json-key-topic",
      "KeyType": "Json",
      "ExportFileName": "test_json_key_topic.json"
    },
    {
      "Name": "test-long-key-topic",
      "KeyType": "Long",
      "ExportFileName": "test_long_key_topic.json"
    }
  ]
}
```
