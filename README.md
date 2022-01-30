![KafkaSnapshot](logo_s.png)
# KafkaSnapshot
Tool that allows to read current data snapshot from Apache Kafka topic to the file.

Supports:
* Compacting
* Key filtering
* Filtering with start and end offsets
* String keys (optionally as json) and long keys. Also Key field could be ignored (e.g. key is null or not exists it topic)
* Multi-partition topics
* SASL authentication mechanism

By default messages should contains JSON data. Simple strings also supported (see ExportRawMessage parameter).

![Details](Case1.PNG)

Config example:

```yaml
  "BootstrapServersConfiguration": {
    "BootstrapServers": [ "test" ],
    "Username": "user123",
    "Password": "pwd123",
    "SecurityProtocol": "SaslPlaintext",
    "SASLMechanism": "ScramSha512"
  },
  "TopicWatermarkLoaderConfiguration": {
    "AdminClientTimeout": "00:00:05"
  },
  "SnapshotLoaderConfiguration": {
    "DateOffsetTimeout": "00:00:05"
  },
  "LoaderToolConfiguration": {
    "UseConcurrentLoad": true,
    "Topics": [
      {
        "Name": "topic1",
        "KeyType": "Json",
        "Compacting": "On",
        "ExportFileName": "topic1.json",
        "FilterType": "Equals",
        "FilterValue": "{\"value\": 1 }",
        "OffsetStartDate": "09.01.2021 12:12:12",
        "OffsetEndDate": "09.01.2021 14:12:12",
        "ExportRawMessage": true
      },
      {
        "Name": "topic2",
        "KeyType": "String",
        "Compacting": "Off",
        "ExportFileName": "topic2.json"
      },
      {
        "Name": "topic3",
        "KeyType": "Long",
        "Compacting": "Off",
        "ExportFileName": "topic3.json",
        "FilterType": "Equals",
        "FilterValue": 42
      },
      {
        "Name": "topic4",
        "KeyType": "Ignored",
        "Compacting": "Off",
        "ExportFileName": "topic4.json"
      }
    ]
  }
```

| Parameter name | Description   |
| -------------- | ------------- |
| AdminClientTimeout | Cluster metadata loading timeout |
| DateOffsetTimeout | Searching offset by date timeout |
| BootstrapServers | List of kafka cluster servers, like "kafka-test:9092"  |
| Username | SASL username (optional)  |
| Password | SASL password (optional)  |
| SecurityProtocol | Protocol used to communicate with brokers (Plaintext,Ssl,SaslPlaintext,SaslSsl) (optional)  |
| SASLMechanism | SASL mechanism to use for authentication (Gssapi,Plain,ScramSha256,ScramSha512,OAuthBearer) (optional)  |
| UseConcurrentLoad | Loads data in concurrent mode or one by one |
| Name           | Apache Kafka topic name |
| KeyType        | Apache Kafka topic key representation (Json,String,Long,Ignored) |
| Compacting     | Use compacting by key or not (On,Off). Not supported for Ignored keyType |
| ExportFileName | File name for exported data  |
| FilterType | Equals or None (optional)  |
| FilterValue | Sample value for filtering (if FilterType sets as 'Equals') |
| OffsetStartDate | First message date (optional). Use to skip old messages in large topics|
| OffsetEndDate | Message date top limit (optional). Use to limit filtering messages in large topics|
| ExportRawMessage | If true - export will write message as raw string without converting to formatted json (optional)|
