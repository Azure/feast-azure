import os
import tempfile
import uuid
from typing import Optional, Tuple

import pytest
from pytest_redis.executor import RedisExecutor

from feast import Client
from feast_spark import Client as SparkClient
from tests.e2e.fixtures.statsd_stub import StatsDServer


@pytest.fixture
def feast_client(
    pytestconfig,
    ingestion_job_jar,
    redis_server: RedisExecutor,
    statsd_server: StatsDServer,
    feast_core: Tuple[str, int],
    feast_serving: Tuple[str, int],
    local_staging_path,
    feast_jobservice: Optional[Tuple[str, int]],
    enable_auth,
):
    if feast_jobservice is None:
        job_service_env = dict()
    else:
        job_service_env = dict(
            job_service_url=f"{feast_jobservice[0]}:{feast_jobservice[1]}"
        )

    if pytestconfig.getoption("env") == "local":
        import pyspark

        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=redis_server.host,
            redis_port=redis_server.port,
            spark_staging_location=os.path.join(local_staging_path, "spark"),
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            ingestion_drop_invalid_rows=True,
            statsd_enabled=True,
            statsd_host=statsd_server.host,
            statsd_port=statsd_server.port,
            **job_service_env,
        )

    elif pytestconfig.getoption("env") == "gcloud":
        c = Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            spark_staging_location=os.path.join(local_staging_path, "dataproc"),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=pytestconfig.getoption("redis_url").split(":")[0],
            redis_port=pytestconfig.getoption("redis_url").split(":")[1],
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            ingestion_drop_invalid_rows=True,
            grpc_connection_timeout=30,
            enable_auth=pytestconfig.getoption("enable_auth"),
            **job_service_env,
        )
    elif pytestconfig.getoption("env") == "aws":
        c = Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="emr",
            emr_cluster_id=pytestconfig.getoption("emr_cluster_id"),
            emr_region=pytestconfig.getoption("emr_region"),
            spark_staging_location=os.path.join(local_staging_path, "emr"),
            emr_log_location=os.path.join(local_staging_path, "emr_logs"),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=pytestconfig.getoption("redis_url").split(":")[0],
            redis_port=pytestconfig.getoption("redis_url").split(":")[1],
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            ingestion_drop_invalid_rows=True,
            enable_auth=pytestconfig.getoption("enable_auth"),
        )
    elif pytestconfig.getoption("env") == "k8s":
        c = Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            spark_staging_location=os.path.join(local_staging_path, "k8s"),
            enable_auth=pytestconfig.getoption("enable_auth"),
            **job_service_env,
        )
    elif pytestconfig.getoption("env") == "synapse":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="synapse",
            azure_synapse_dev_url = pytestconfig.getoption("azure_synapse_dev_url"),
            azure_synapse_pool_name = pytestconfig.getoption("azure_synapse_pool_name"),
            azure_synapse_datalake_dir = pytestconfig.getoption("azure_synapse_datalake_dir"),
            spark_staging_location=os.path.join(local_staging_path, "synapse"),
            azure_blob_account_name=pytestconfig.getoption("azure_blob_account_name"),
            azure_blob_account_access_key=pytestconfig.getoption("azure_blob_account_access_key"),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=pytestconfig.getoption("redis_url").split(":")[0],
            redis_port=pytestconfig.getoption("redis_url").split(":")[1],
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
        )
    else:
        raise KeyError(f"Unknown environment {pytestconfig.getoption('env')}")

    c.set_project(pytestconfig.getoption("feast_project"))
    return c


@pytest.fixture
def feast_spark_client(feast_client: Client) -> SparkClient:
    return SparkClient(feast_client)


@pytest.fixture
def tfrecord_feast_client(
    pytestconfig,
    feast_core: Tuple[str, int],
    local_staging_path,
    feast_jobservice: Optional[Tuple[str, int]],
    enable_auth,
):
    if feast_jobservice is None:
        job_service_env = dict()
    else:
        job_service_env = dict(
            job_service_url=f"{feast_jobservice[0]}:{feast_jobservice[1]}"
        )

    if pytestconfig.getoption("env") == "local":
        import pyspark

        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_staging_location=os.path.join(local_staging_path, "spark"),
            historical_feature_output_format="tfrecord",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            **job_service_env,
        )

    elif pytestconfig.getoption("env") == "gcloud":
        c = Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            spark_staging_location=os.path.join(local_staging_path, "dataproc"),
            historical_feature_output_format="tfrecord",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            ingestion_drop_invalid_rows=True,
            **job_service_env,
        )
    elif pytestconfig.getoption("env") == "aws":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            spark_launcher="emr",
            emr_cluster_id=pytestconfig.getoption("emr_cluster_id"),
            emr_region=pytestconfig.getoption("emr_region"),
            spark_staging_location=os.path.join(local_staging_path, "emr"),
            emr_log_location=os.path.join(local_staging_path, "emr_logs"),
            historical_feature_output_format="tfrecord",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
        )
    elif pytestconfig.getoption("env") == "k8s":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            spark_launcher="k8s",
            spark_staging_location=os.path.join(local_staging_path, "k8s"),
            historical_feature_output_format="tfrecord",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
            **job_service_env,
        )
    elif pytestconfig.getoption("env") == "synapse":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            spark_launcher="synapse",
            azure_synapse_dev_url = pytestconfig.getoption("azure_synapse_dev_url"),
            azure_synapse_pool_name = pytestconfig.getoption("azure_synapse_pool_name"),
            azure_synapse_datalake_dir = pytestconfig.getoption("azure_synapse_datalake_dir"),
            spark_staging_location=os.path.join(local_staging_path, "synapse"),
            azure_blob_account_name=pytestconfig.getoption("azure_blob_account_name"),
            azure_blob_account_access_key=pytestconfig.getoption("azure_blob_account_access_key"),
            historical_feature_output_format="tfrecord",
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
        )
    else:
        raise KeyError(f"Unknown environment {pytestconfig.getoption('env')}")

    c.set_project(pytestconfig.getoption("feast_project"))
    return c


@pytest.fixture(scope="session")
def global_staging_path(pytestconfig):
    if pytestconfig.getoption("env") == "local" and not pytestconfig.getoption(
        "staging_path", ""
    ):
        tmp_path = tempfile.mkdtemp()
        return f"file://{tmp_path}"

    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="function")
def local_staging_path(global_staging_path):
    return os.path.join(global_staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="session")
def ingestion_job_jar(pytestconfig, project_root, project_version):
    default_path = (
        project_root
        / "spark"
        / "ingestion"
        / "target"
        / f"feast-ingestion-spark-{project_version}.jar"
    )

    return pytestconfig.getoption("ingestion_jar") or default_path
