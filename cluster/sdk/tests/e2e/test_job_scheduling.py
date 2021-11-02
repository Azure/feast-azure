import hashlib
import uuid

import pytest as pytest
from kubernetes import client, config

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast_spark import Client as SparkClient


@pytest.mark.env("k8s")
def test_schedule_batch_ingestion_jobs(
    pytestconfig, feast_client: Client, feast_spark_client: SparkClient
):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="gs://example/feast/*",
        event_timestamp_column="datetime_col",
        created_timestamp_column="timestamp",
        date_partition_column="datetime",
    )
    feature_table = FeatureTable(
        name=f"schedule_{str(uuid.uuid4())}".replace("-", "_"),
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=batch_source,
    )
    feast_client.apply(entity)
    feast_client.apply(feature_table)

    feast_spark_client.schedule_offline_to_online_ingestion(
        feature_table, 1, "0 0 * * *"
    )
    config.load_incluster_config()
    k8s_api = client.CustomObjectsApi()

    def get_scheduled_spark_application():
        job_hash = hashlib.md5(
            f"{feast_client.project}-{feature_table.name}".encode()
        ).hexdigest()
        resource_name = f"feast-{job_hash}"

        return k8s_api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=pytestconfig.getoption("k8s_namespace"),
            plural="scheduledsparkapplications",
            name=resource_name,
        )

    response = get_scheduled_spark_application()
    assert response["spec"]["schedule"] == "0 0 * * *"
    feast_spark_client.schedule_offline_to_online_ingestion(
        feature_table, 1, "1 0 * * *"
    )
    response = get_scheduled_spark_application()
    assert response["spec"]["schedule"] == "1 0 * * *"

    feast_spark_client.unschedule_offline_to_online_ingestion(feature_table)
