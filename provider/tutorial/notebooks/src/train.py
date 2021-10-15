# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import argparse
from feast import FeatureStore, RepoConfig
from feast.repo_config import SqlServerOfflineStoreConfig, RedisOnlineStoreConfig

parser = argparse.ArgumentParser()
parser.add_argument("--registry")
args = parser.parse_args()

connection_string = os.getenv('FEAST_HIST_CONN')
redis_endpoint = os.getenv('FEAST_REDIS_CONN')

repo_cfg = RepoConfig(
        project = "feast_aml_demo",
        provider = "local",
        registry = args.registry,
        offline_store = SqlServerOfflineStoreConfig(connection_string=connection_string),
        online_store = RedisOnlineStoreConfig(connection_string=redis_endpoint)
    )

store = FeatureStore(config=repo_cfg)

sql_job = store.get_historical_features(
            entity_df="SELECT * FROM orders",
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:acc_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
            ],
        )

training_df = sql_job.to_df()
print(training_df.head())

