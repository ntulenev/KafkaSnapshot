# KafkaSnapshot
Tool that allows to read current data snapshot from kafka topic with compacting

Current version is optimized for <string,string> key-value messages and skips topics with NULL keys.

![Details](Details.PNG)

Config with topics for export

```yaml
  "LoaderToolConfiguration": {
    "MetadataTimeout": 1000,
    "BootstrapServers": [
    ],
    "Topics": [
    ]
  }
```
