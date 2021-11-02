from feast import Client

import os
# Connect to an existing Feast Core deployment

ipaddress='<<TODO- Put your Cluster IP here>>:6565'
client = Client(core_url=ipaddress)
print('Ip used: ' + ipaddress)

# Ensure that your client is connected by printing out some feature tables
client.list_feature_tables()

from feast import Client, Feature, Entity, ValueType, FeatureTable
from feast.data_source import FileSource
from feast.data_format import ParquetFormat


demo_data_location = "wasbs://feasttest@feaststore.blob.core.windows.net/"
driver_statistics_source_uri = os.path.join(demo_data_location, "driver_statistics")
driver_trips_source_uri = os.path.join(demo_data_location, "driver_trips")



driver_id = Entity(name="driver_id", description="Driver identifier", value_type=ValueType.INT64)

# Daily updated features 
acc_rate = Feature("acc_rate", ValueType.FLOAT)
conv_rate = Feature("conv_rate", ValueType.FLOAT)
avg_daily_trips = Feature("avg_daily_trips", ValueType.INT32)

# Real-time updated features
trips_today = Feature("trips_today", ValueType.INT32)

driver_statistics = FeatureTable(
    name = "driver_statistics",
    entities = ["driver_id"],
    features = [
        acc_rate,
        conv_rate,
        avg_daily_trips
    ],
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url=driver_statistics_source_uri,
        date_partition_column="date"
    )
)



driver_trips = FeatureTable(
    name = "driver_trips",
    entities = ["driver_id"],
    features = [
        trips_today
    ],
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url=driver_trips_source_uri,
        date_partition_column="date"
    )
)

client.apply(driver_id)
client.apply(driver_statistics)
client.apply(driver_trips)

print(client.get_feature_table("driver_statistics").to_yaml())
print(client.get_feature_table("driver_trips").to_yaml())


