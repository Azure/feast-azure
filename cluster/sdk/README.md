# Feast Spark

This is a fork of [feast-spark pacakge](https://github.com/feast-dev/feast-spark). This pacakge can be installed by executing this line (tested with Python 3.8 and Linux):

```bash
pip install git+https://github.com/xiaoyongzhu/feast-spark.git#subdirectory=python
```

Contains
* Spark ingestion jobs for [Feast](https://github.com/feast-dev/feast)
* Feast Job Service
* Feast Python SDK Spark extensions 

Usage:

```python

import feast_spark
import feast

client = feast.Client()

client.set_project("project1")
entity = feast.Entity(
    name="driver_car_id",
    description="Car driver id",
    value_type=ValueType.STRING,
    labels={"team": "matchmaking"},
)

# Create Feature Tables using Feast SDK
batch_source = feast.FileSource(
    file_format=ParquetFormat(),
    file_url="file://feast/*",
    event_timestamp_column="ts_col",
    created_timestamp_column="timestamp",
    date_partition_column="date_partition_col",
)

stream_source = feast.KafkaSource(
    bootstrap_servers="localhost:9094",
    message_format=ProtoFormat("class.path"),
    topic="test_topic",
    event_timestamp_column="ts_col",
)

ft = feast.FeatureTable(
    name="my-feature-table-1",
    features=[
        Feature(name="fs1-my-feature-1", dtype=ValueType.INT64),
        Feature(name="fs1-my-feature-2", dtype=ValueType.STRING),
        Feature(name="fs1-my-feature-3", dtype=ValueType.STRING_LIST),
        Feature(name="fs1-my-feature-4", dtype=ValueType.BYTES_LIST),
    ],
    entities=["fs1-my-entity-1"],
    labels={"team": "matchmaking"},
    batch_source=batch_source,
    stream_source=stream_source,
)

# Register objects in Feast
client.apply(entity, ft)

# Start spark streaming ingestion job that reads from kafka and writes to the online store
feast_spark.Client(client).start_stream_to_online_ingestion(ft)
```

Build and push to BLOB storage

In order to build the Spark Ingestion jar and copy it to BLOB storage, you have to set these 3 environment variables:

```bash
export VERSION=latest
export REGISTRY=your_registry_name
export AZURE_STORAGE_CONNECTION_STRING="your_azure_storage_connection_string"
```
