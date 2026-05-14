<p align="center">
  <img src="logo_s.png" alt="KafkaSnapshot logo" width="96" />
</p>

# KafkaSnapshot

KafkaSnapshot is a .NET utility for exporting the current data snapshot from Apache Kafka topics to JSON files.

It is useful when you need to inspect topic state, capture data for diagnostics, export compacted views by key, or take a reproducible snapshot from a selected offset/date range.

## Highlights

- Export one or many Kafka topics to JSON files.
- Read multi-partition topics with optional concurrent loading.
- Compact messages by key to keep the latest value per key.
- Filter by key using equality, contains, greater-or-equal, and less-or-equal rules.
- Limit exported data by start/end timestamps or selected partition ids.
- Support JSON, string, long, and ignored keys.
- Decode string, MessagePack, MessagePack LZ4 block, and Base64 payloads.
- Stream large exports directly to files to reduce memory pressure.
- Sort exported messages by timestamp or partition when compacting is disabled.
- Connect to secured clusters with SASL authentication.

## How It Works

KafkaSnapshot loads topic watermarks, reads matching messages from the selected partitions, applies optional filters/compacting/sorting, and writes the result to JSON.

![KafkaSnapshot flow](Case1.PNG)

## Requirements

- .NET 10 SDK
- Access to an Apache Kafka cluster

## Quick Start

The repository uses the modern `.slnx` solution format.

```powershell
dotnet restore src\KafkaSnapshot.slnx
dotnet build src\KafkaSnapshot.slnx --no-restore
dotnet test src\KafkaSnapshot.slnx --no-build
```

Configure the utility:

```powershell
notepad src\Utility\appsettings.json
```

Run it:

```powershell
dotnet run --project src\Utility\Utility.csproj
```

Exported files are written to the current working directory by default. Set `JsonFileDataExporterConfiguration.OutputDirectory` to write snapshots to a dedicated folder.

## Configuration

The main configuration file is [src/Utility/appsettings.json](src/Utility/appsettings.json). An optional JSON schema for editor completion and validation is available at [src/Utility/appsettings.schema.json](src/Utility/appsettings.schema.json).

A minimal configuration looks like this:

```json
{
  "BootstrapServersConfiguration": {
    "BootstrapServers": [ "kafka-test:9092" ]
  },
  "TopicWatermarkLoaderConfiguration": {
    "AdminClientTimeout": "00:00:05"
  },
  "SnapshotLoaderConfiguration": {
    "DateOffsetTimeout": "00:00:05",
    "SearchSinglePartition": false,
    "MaxConcurrentPartitions": 4
  },
  "JsonFileDataExporterConfiguration": {
    "UseFileStreaming": true,
    "OutputDirectory": "snapshots"
  },
  "LoaderToolConfiguration": {
    "UseConcurrentLoad": true,
    "GlobalMessageSort": "Time",
    "GlobalSortOrder": "Ascending",
    "Topics": [
      {
        "Name": "orders",
        "KeyType": "Json",
        "Compacting": "On",
        "ExportFileName": "orders.json"
      }
    ]
  }
}
```

## Common Recipes

### Export a Topic as Raw Strings

Use this when messages are plain strings and should not be formatted as JSON objects.

```json
{
  "Name": "events.raw",
  "KeyType": "String",
  "Compacting": "Off",
  "ExportRawMessage": true,
  "ExportFileName": "events.raw.json"
}
```

### Export Only Selected Partitions

```json
{
  "Name": "orders",
  "KeyType": "Json",
  "Compacting": "Off",
  "PartitionsIds": [ 0, 2 ],
  "ExportFileName": "orders.partitions-0-2.json"
}
```

### Export a Date Range

Use ISO 8601 values with an explicit UTC designator or offset.

```json
{
  "Name": "orders",
  "KeyType": "Json",
  "Compacting": "Off",
  "OffsetStartDate": "2021-09-01T12:12:12Z",
  "OffsetEndDate": "2021-09-01T14:12:12+02:00",
  "ExportFileName": "orders.range.json"
}
```

### Filter by Key

```json
{
  "Name": "users",
  "KeyType": "Long",
  "Compacting": "Off",
  "FilterKeyType": "GreaterOrEquals",
  "FilterKeyValue": 1000,
  "ExportFileName": "users.filtered.json"
}
```

Filter restrictions:

- `Contains` can be used only with string keys.
- `GreaterOrEquals` and `LessOrEquals` can be used only with long keys.
- `Compacting` is not supported when `KeyType` is `Ignored`.

### Decode MessagePack or Base64 Payloads

```json
{
  "Name": "packed-events",
  "KeyType": "Long",
  "MessageEncoderRule": "MessagePack",
  "ExportFileName": "packed-events.json"
}
```

Supported `MessageEncoderRule` values:

- `String`
- `MessagePack`
- `MessagePackLz4Block`
- `Base64`

## Full Example

```json
{
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
    "DateOffsetTimeout": "00:00:05",
    "SearchSinglePartition": false,
    "MaxConcurrentPartitions": 4
  },
  "JsonFileDataExporterConfiguration": {
    "UseFileStreaming": true,
    "OutputDirectory": "snapshots"
  },
  "LoaderToolConfiguration": {
    "UseConcurrentLoad": true,
    "GlobalMessageSort": "Time",
    "GlobalSortOrder": "Ascending",
    "Topics": [
      {
        "Name": "topic1",
        "KeyType": "Json",
        "Compacting": "On",
        "ExportFileName": "topic1.json",
        "FilterKeyType": "Equals",
        "FilterKeyValue": "{\"value\": 1 }",
        "OffsetStartDate": "2021-09-01T12:12:12Z",
        "OffsetEndDate": "2021-09-01T14:12:12+02:00",
        "ExportRawMessage": true,
        "PartitionsIds": [ 0, 2 ]
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
        "FilterKeyType": "Equals",
        "FilterKeyValue": 42
      },
      {
        "Name": "topic4",
        "KeyType": "Ignored",
        "Compacting": "Off",
        "ExportFileName": "topic4.json"
      },
      {
        "Name": "topic5",
        "KeyType": "String",
        "Compacting": "Off",
        "FilterKeyType": "Contains",
        "FilterKeyValue": "Test",
        "ExportRawMessage": true,
        "ExportFileName": "topic5.json"
      },
      {
        "Name": "topic6",
        "KeyType": "Long",
        "Compacting": "Off",
        "FilterKeyType": "LessOrEquals",
        "FilterKeyValue": 3,
        "ExportFileName": "topic6.json"
      },
      {
        "Name": "topic7",
        "KeyType": "Long",
        "Compacting": "Off",
        "FilterKeyType": "GreaterOrEquals",
        "FilterKeyValue": 3,
        "ExportFileName": "topic7.json"
      },
      {
        "Name": "topic8",
        "KeyType": "Long",
        "ExportFileName": "topic8.json",
        "MessageEncoderRule": "MessagePack"
      },
      {
        "Name": "topic9",
        "KeyType": "Long",
        "ExportFileName": "topic9.json",
        "MessageEncoderRule": "MessagePackLz4Block"
      },
      {
        "Name": "topic10",
        "KeyType": "Long",
        "ExportRawMessage": true,
        "MessageEncoderRule": "Base64",
        "ExportFileName": "topic10.json"
      }
    ]
  }
}
```

## Configuration Reference

| Parameter | Description |
| --- | --- |
| `BootstrapServers` | Kafka bootstrap servers, for example `kafka-test:9092`. |
| `Username` | SASL username. Optional. |
| `Password` | SASL password. Optional. |
| `SecurityProtocol` | Broker communication protocol: `Plaintext`, `Ssl`, `SaslPlaintext`, or `SaslSsl`. Optional. |
| `SASLMechanism` | SASL mechanism: `Gssapi`, `Plain`, `ScramSha256`, `ScramSha512`, or `OAuthBearer`. Optional. |
| `AdminClientTimeout` | Timeout for loading cluster metadata. |
| `DateOffsetTimeout` | Timeout for finding Kafka offsets by timestamp. |
| `SearchSinglePartition` | Stops partition traversal after the first partition with suitable data. |
| `MaxConcurrentPartitions` | Maximum number of topic partitions processed concurrently. Optional; when omitted, all partitions are processed concurrently. |
| `UseFileStreaming` | Writes loaded data directly through `FileStream` to reduce memory usage on large exports. Works best with sorting disabled. |
| `OutputDirectory` | Directory for exported files. Optional; when omitted, files are written to the current working directory. |
| `UseConcurrentLoad` | Loads configured topics concurrently instead of one by one. |
| `GlobalMessageSort` | Message metadata field used for sorting: `Time` or `Partition`. Optional. Applies only to topics without compacting. |
| `GlobalSortOrder` | Sort order: `Ascending`, `Descending`, or `None`. Legacy aliases `Ask`, `Desk`, and `No` are also supported. Applies only to topics without compacting. |
| `Name` | Kafka topic name. |
| `KeyType` | Kafka key representation: `Json`, `String`, `Long`, or `Ignored`. |
| `Compacting` | Enables or disables compacting by key: `On` or `Off`. Not supported for `Ignored` keys. |
| `ExportFileName` | Output JSON file name for the topic. |
| `FilterKeyType` | Key filter type: `None`, `Equals`, `Contains`, `GreaterOrEquals`, or `LessOrEquals`. Optional. |
| `FilterKeyValue` | Value used by the selected key filter. |
| `OffsetStartDate` | First message timestamp to include. Use ISO 8601 with an explicit offset, for example `2021-09-01T12:12:12Z` or `2021-09-01T14:12:12+02:00`. Optional. |
| `OffsetEndDate` | Upper timestamp limit. Use ISO 8601 with an explicit offset, for example `2021-09-01T12:12:12Z` or `2021-09-01T14:12:12+02:00`. Optional. |
| `ExportRawMessage` | Exports message values as raw strings instead of formatting them as JSON. Optional. |
| `PartitionsIds` | Partition id filter. Optional. |
| `MessageEncoderRule` | Message body decoder. Defaults to `String`; also supports `MessagePack`, `MessagePackLz4Block`, and `Base64`. |

## Export Format

By default, KafkaSnapshot expects message values to contain JSON data. Simple strings are supported when `ExportRawMessage` is enabled.

![Kafka data example](OriginalData.png)

```json
[
  {
    "Key": {
      "ItemId": 1
    },
    "Value": {
      "Data": 100
    },
    "Meta": {
      "Timestamp": "2022-04-20T17:04:43.307Z",
      "Partition": 0,
      "Offset": 0
    }
  },
  {
    "Key": {
      "ItemId": 1
    },
    "Value": {
      "Data": 200
    },
    "Meta": {
      "Timestamp": "2022-04-20T17:04:55.197Z",
      "Partition": 0,
      "Offset": 1
    }
  },
  {
    "Key": {
      "ItemId": 2
    },
    "Value": {
      "Data": 100
    },
    "Meta": {
      "Timestamp": "2022-04-20T17:05:05.81Z",
      "Partition": 0,
      "Offset": 2
    }
  }
]
```

| Field | Description |
| --- | --- |
| `Key` | Kafka message key. Optional when `KeyType` is `Ignored`. |
| `Value` | Kafka message value. |
| `Meta` | Kafka message metadata. |
| `Meta.Timestamp` | Kafka message creation timestamp. |
| `Meta.Partition` | Kafka partition id. |
| `Meta.Offset` | Kafka partition offset. |

## Notes for Existing Users

Recent versions use `DateTimeOffset` for date-based offsets and exported metadata timestamps.

- `OffsetStartDate` and `OffsetEndDate` should be configured as ISO 8601 strings with an explicit UTC designator or offset.
- `KafkaMetadata.Timestamp` is now a `DateTimeOffset`.
- `DateFilterRange.StartDate`, `DateFilterRange.EndDate`, `LoadingTopic.OffsetDate`, `LoadingTopic.EndOffsetDate`, `TopicConfiguration.OffsetStartDate`, and `TopicConfiguration.OffsetEndDate` are now `DateTimeOffset` values.
- `PartitionWatermark.TopicName` was renamed to `PartitionWatermark.Topic` because it stores the full loading topic configuration, not only the topic name.
