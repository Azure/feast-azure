import os
import tempfile
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List, Optional, Union
from urllib.parse import urlparse, urlunparse

from feast.config import Config
from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, DataSource, FileSource, KafkaSource
from feast.feature_table import FeatureTable
from feast.staging.storage_client import get_staging_client
from feast.value_type import ValueType
from feast_spark.constants import ConfigOptions as opt
from feast_spark.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    ScheduledBatchIngestionJobParameters,
    SparkJob,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

if TYPE_CHECKING:
    from feast_spark.client import Client


def _standalone_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import standalone

    return standalone.StandaloneClusterLauncher(
        config.get(opt.SPARK_STANDALONE_MASTER), config.get(opt.SPARK_HOME),
    )


def _dataproc_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import gcloud

    return gcloud.DataprocClusterLauncher(
        cluster_name=config.get(opt.DATAPROC_CLUSTER_NAME),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        region=config.get(opt.DATAPROC_REGION),
        project_id=config.get(opt.DATAPROC_PROJECT),
        executor_instances=config.get(opt.DATAPROC_EXECUTOR_INSTANCES),
        executor_cores=config.get(opt.DATAPROC_EXECUTOR_CORES),
        executor_memory=config.get(opt.DATAPROC_EXECUTOR_MEMORY),
    )


def _emr_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import aws

    def _get_optional(option):
        if config.exists(option):
            return config.get(option)

    return aws.EmrClusterLauncher(
        region=config.get(opt.EMR_REGION),
        existing_cluster_id=_get_optional(opt.EMR_CLUSTER_ID),
        new_cluster_template_path=_get_optional(opt.EMR_CLUSTER_TEMPLATE_PATH),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        emr_log_location=config.get(opt.EMR_LOG_LOCATION),
    )


def _k8s_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import k8s

    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    return k8s.KubernetesJobLauncher(
        namespace=config.get(opt.SPARK_K8S_NAMESPACE),
        resource_template_path=config.get(opt.SPARK_K8S_JOB_TEMPLATE_PATH, None),
        staging_location=staging_location,
        incluster=config.getboolean(opt.SPARK_K8S_USE_INCLUSTER_CONFIG),
        staging_client=get_staging_client(staging_uri.scheme, config),
        # azure-related arguments are None if not using Azure blob storage
        azure_account_name=config.get(opt.AZURE_BLOB_ACCOUNT_NAME, None),
        azure_account_key=config.get(opt.AZURE_BLOB_ACCOUNT_ACCESS_KEY, None),
    )


def _synapse_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import synapse

    return synapse.SynapseJobLauncher(
        synapse_dev_url=config.get(opt.AZURE_SYNAPSE_DEV_URL),
        pool_name=config.get(opt.AZURE_SYNAPSE_POOL_NAME),
        datalake_dir=config.get(opt.AZURE_SYNAPSE_DATALAKE_DIR),
        executor_size=config.get(opt.AZURE_SYNAPSE_EXECUTOR_SIZE),
        executors=int(config.get(opt.AZURE_SYNAPSE_EXECUTORS))
    )


_launchers = {
    "standalone": _standalone_launcher,
    "dataproc": _dataproc_launcher,
    "emr": _emr_launcher,
    "k8s": _k8s_launcher,
    'synapse': _synapse_launcher,
}


def resolve_launcher(config: Config) -> JobLauncher:
    return _launchers[config.get(opt.SPARK_LAUNCHER)](config)


def _source_to_argument(source: DataSource, config: Config):
    common_properties = {
        "field_mapping": dict(source.field_mapping),
        "event_timestamp_column": source.event_timestamp_column,
        "created_timestamp_column": source.created_timestamp_column,
        "date_partition_column": source.date_partition_column,
    }

    properties = {**common_properties}

    if isinstance(source, FileSource):
        properties["path"] = source.file_options.file_url
        properties["format"] = dict(
            json_class=source.file_options.file_format.__class__.__name__
        )
        return {"file": properties}

    if isinstance(source, BigQuerySource):
        project, dataset_and_table = source.bigquery_options.table_ref.split(":")
        dataset, table = dataset_and_table.split(".")
        properties["project"] = project
        properties["dataset"] = dataset
        properties["table"] = table
        if config.exists(opt.SPARK_BQ_MATERIALIZATION_PROJECT) and config.exists(
            opt.SPARK_BQ_MATERIALIZATION_DATASET
        ):
            properties["materialization"] = dict(
                project=config.get(opt.SPARK_BQ_MATERIALIZATION_PROJECT),
                dataset=config.get(opt.SPARK_BQ_MATERIALIZATION_DATASET),
            )

        return {"bq": properties}

    if isinstance(source, KafkaSource):
        properties["bootstrap_servers"] = source.kafka_options.bootstrap_servers
        properties["topic"] = source.kafka_options.topic
        properties["format"] = {
            **source.kafka_options.message_format.__dict__,
            "json_class": source.kafka_options.message_format.__class__.__name__,
        }
        return {"kafka": properties}

    raise NotImplementedError(f"Unsupported Datasource: {type(source)}")


def _feature_table_to_argument(
    client: "Client", project: str, feature_table: FeatureTable, use_gc_threshold=True,
):
    max_age = feature_table.max_age.ToSeconds() if feature_table.max_age else None
    if use_gc_threshold:
        try:
            gc_threshold = int(feature_table.labels["gcThresholdSec"])
        except (KeyError, ValueError, TypeError):
            pass
        else:
            max_age = max(max_age or 0, gc_threshold)

    return {
        "features": [
            {"name": f.name, "type": ValueType(f.dtype).name}
            for f in feature_table.features
        ],
        "project": project,
        "name": feature_table.name,
        "entities": [
            {
                "name": n,
                "type": client.feature_store.get_entity(n, project=project).value_type,
            }
            for n in feature_table.entities
        ],
        "max_age": max_age,
        "labels": dict(feature_table.labels),
    }


def start_historical_feature_retrieval_spark_session(
    client: "Client",
    project: str,
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
):
    from pyspark.sql import SparkSession

    from feast_spark.pyspark.historical_feature_retrieval_job import (
        retrieve_historical_features,
    )

    spark_session = SparkSession.builder.getOrCreate()
    return retrieve_historical_features(
        spark=spark_session,
        entity_source_conf=_source_to_argument(entity_source, client.config),
        feature_tables_sources_conf=[
            _source_to_argument(feature_table.batch_source, client.config)
            for feature_table in feature_tables
        ],
        feature_tables_conf=[
            _feature_table_to_argument(
                client, project, feature_table, use_gc_threshold=False
            )
            for feature_table in feature_tables
        ],
    )


def start_historical_feature_retrieval_job(
    client: "Client",
    project: str,
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
    output_format: str,
    output_path: str,
) -> RetrievalJob:
    launcher = resolve_launcher(client.config)
    feature_sources = [
        _source_to_argument(
            replace_bq_table_with_joined_view(feature_table, entity_source),
            client.config,
        )
        for feature_table in feature_tables
    ]

    extra_packages = []
    if output_format == "tfrecord":
        extra_packages.append("com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.0")

    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            entity_source=_source_to_argument(entity_source, client.config),
            feature_tables_sources=feature_sources,
            feature_tables=[
                _feature_table_to_argument(
                    client, project, feature_table, use_gc_threshold=False
                )
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
            extra_packages=extra_packages,
            checkpoint_path=client.config.get(opt.CHECKPOINT_PATH),
        )
    )


def replace_bq_table_with_joined_view(
    feature_table: FeatureTable, entity_source: Union[FileSource, BigQuerySource],
) -> Union[FileSource, BigQuerySource]:
    """
    Applies optimization to historical retrieval. Instead of pulling all data from Batch Source,
    with this optimization we join feature values & entities on Data Warehouse side (improving data locality).
    Several conditions should be met to enable this optimization:
    * entities are staged to BigQuery
    * feature values are in in BigQuery
    * Entity columns are not mapped (ToDo: fix this limitation)
    :return: replacement for feature source
    """
    if not isinstance(feature_table.batch_source, BigQuerySource):
        return feature_table.batch_source

    if not isinstance(entity_source, BigQuerySource):
        return feature_table.batch_source

    if any(
        entity in feature_table.batch_source.field_mapping
        for entity in feature_table.entities
    ):
        return feature_table.batch_source

    return create_bq_view_of_joined_features_and_entities(
        feature_table.batch_source, entity_source, feature_table.entities,
    )


def table_reference_from_string(table_ref: str):
    """
    Parses reference string with format "{project}:{dataset}.{table}" into bigquery.TableReference
    """
    from google.cloud import bigquery

    project, dataset_and_table = table_ref.split(":")
    dataset, table_id = dataset_and_table.split(".")
    return bigquery.TableReference(
        bigquery.DatasetReference(project, dataset), table_id
    )


def create_bq_view_of_joined_features_and_entities(
    source: BigQuerySource, entity_source: BigQuerySource, entity_names: List[str]
) -> BigQuerySource:
    """
    Creates BQ view that joins tables from `source` and `entity_source` with join key derived from `entity_names`.
Returns BigQuerySource with reference to created view. The BQ view will be created in the same BQ dataset as `entity_source`.
    """
    from google.cloud import bigquery

    bq_client = bigquery.Client()

    source_ref = table_reference_from_string(source.bigquery_options.table_ref)
    entities_ref = table_reference_from_string(entity_source.bigquery_options.table_ref)

    destination_ref = bigquery.TableReference(
        bigquery.DatasetReference(entities_ref.project, entities_ref.dataset_id),
        f"_view_{source_ref.table_id}_{datetime.now():%Y%m%d%H%M%s}",
    )

    view = bigquery.Table(destination_ref)

    join_template = """
    SELECT source.* FROM
    `{entities.project}.{entities.dataset_id}.{entities.table_id}` entities
    JOIN
    `{source.project}.{source.dataset_id}.{source.table_id}` source
    ON
    ({entity_key})"""

    view.view_query = join_template.format(
        entities=entities_ref,
        source=source_ref,
        entity_key=" AND ".join([f"source.{e} = entities.{e}" for e in entity_names]),
    )
    view.expires = datetime.now() + timedelta(days=1)
    bq_client.create_table(view)

    return BigQuerySource(
        event_timestamp_column=source.event_timestamp_column,
        created_timestamp_column=source.created_timestamp_column,
        table_ref=f"{view.project}:{view.dataset_id}.{view.table_id}",
        field_mapping=source.field_mapping,
        date_partition_column=source.date_partition_column,
    )


def start_offline_to_online_ingestion(
    client: "Client",
    project: str,
    feature_table: FeatureTable,
    start: datetime,
    end: datetime,
) -> BatchIngestionJob:

    launcher = resolve_launcher(client.config)

    return launcher.offline_to_online_ingestion(
        BatchIngestionJobParameters(
            jar=client.config.get(opt.SPARK_INGESTION_JAR),
            source=_source_to_argument(feature_table.batch_source, client.config),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            start=start,
            end=end,
            redis_host=client.config.get(opt.REDIS_HOST),
            redis_port=bool(client.config.get(opt.REDIS_HOST))
            and client.config.getint(opt.REDIS_PORT),
            redis_ssl=client.config.getboolean(opt.REDIS_SSL),
            redis_auth=client.config.get(opt.REDIS_AUTH),
            bigtable_project=client.config.get(opt.BIGTABLE_PROJECT),
            bigtable_instance=client.config.get(opt.BIGTABLE_INSTANCE),
            cassandra_host=client.config.get(opt.CASSANDRA_HOST),
            cassandra_port=bool(client.config.get(opt.CASSANDRA_HOST))
            and client.config.getint(opt.CASSANDRA_PORT),
            statsd_host=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.get(opt.STATSD_HOST)
            ),
            statsd_port=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.getint(opt.STATSD_PORT)
            ),
            deadletter_path=client.config.get(opt.DEADLETTER_PATH),
            stencil_url=client.config.get(opt.STENCIL_URL),
        )
    )


def schedule_offline_to_online_ingestion(
    client: "Client",
    project: str,
    feature_table: FeatureTable,
    ingestion_timespan: int,
    cron_schedule: str,
):

    launcher = resolve_launcher(client.config)

    launcher.schedule_offline_to_online_ingestion(
        ScheduledBatchIngestionJobParameters(
            jar=client.config.get(opt.SPARK_INGESTION_JAR),
            source=_source_to_argument(feature_table.batch_source, client.config),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            ingestion_timespan=ingestion_timespan,
            cron_schedule=cron_schedule,
            redis_host=client.config.get(opt.REDIS_HOST),
            redis_port=bool(client.config.get(opt.REDIS_HOST))
            and client.config.getint(opt.REDIS_PORT),
            redis_ssl=client.config.getboolean(opt.REDIS_SSL),
            bigtable_project=client.config.get(opt.BIGTABLE_PROJECT),
            bigtable_instance=client.config.get(opt.BIGTABLE_INSTANCE),
            cassandra_host=client.config.get(opt.CASSANDRA_HOST),
            cassandra_port=bool(client.config.get(opt.CASSANDRA_HOST))
            and client.config.getint(opt.CASSANDRA_PORT),
            statsd_host=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.get(opt.STATSD_HOST)
            ),
            statsd_port=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.getint(opt.STATSD_PORT)
            ),
            deadletter_path=client.config.get(opt.DEADLETTER_PATH),
            stencil_url=client.config.get(opt.STENCIL_URL),
        )
    )


def unschedule_offline_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable,
):

    launcher = resolve_launcher(client.config)
    launcher.unschedule_offline_to_online_ingestion(project, feature_table.name)


def get_stream_to_online_ingestion_params(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJobParameters:
    return StreamIngestionJobParameters(
        jar=client.config.get(opt.SPARK_INGESTION_JAR),
        extra_jars=extra_jars,
        source=_source_to_argument(feature_table.stream_source, client.config),
        feature_table=_feature_table_to_argument(client, project, feature_table),
        redis_host=client.config.get(opt.REDIS_HOST),
        redis_port=client.config.getint(opt.REDIS_PORT),
        redis_ssl=client.config.getboolean(opt.REDIS_SSL),
        redis_auth=client.config.get(opt.REDIS_AUTH),
        statsd_host=client.config.getboolean(opt.STATSD_ENABLED)
        and client.config.get(opt.STATSD_HOST),
        statsd_port=client.config.getboolean(opt.STATSD_ENABLED)
        and client.config.getint(opt.STATSD_PORT),
        deadletter_path=client.config.get(opt.DEADLETTER_PATH),
        checkpoint_path=client.config.get(opt.CHECKPOINT_PATH),
        stencil_url=client.config.get(opt.STENCIL_URL),
        drop_invalid_rows=client.config.get(opt.INGESTION_DROP_INVALID_ROWS),
        kafka_sasl_auth=client.config.get(opt.AZURE_EVENTHUB_KAFKA_CONNECTION_STRING),        

    )

def start_stream_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJob:

    launcher = resolve_launcher(client.config)

    return launcher.start_stream_to_online_ingestion(
        get_stream_to_online_ingestion_params(
            client, project, feature_table, extra_jars
        )
    )


def list_jobs(
    include_terminated: bool,
    client: "Client",
    project: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[SparkJob]:
    launcher = resolve_launcher(client.config)
    return launcher.list_jobs(
        include_terminated=include_terminated, table_name=table_name, project=project
    )


def get_job_by_id(job_id: str, client: "Client") -> SparkJob:
    launcher = resolve_launcher(client.config)
    return launcher.get_job_by_id(job_id)


def stage_dataframe(df, event_timestamp_column: str, config: Config) -> FileSource:
    """
    Helper function to upload a pandas dataframe in parquet format to a temporary location (under
    SPARK_STAGING_LOCATION) and return it wrapped in a FileSource.

    Args:
        event_timestamp_column(str): the name of the timestamp column in the dataframe.
        config(Config): feast config.
    """
    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    with tempfile.NamedTemporaryFile() as f:
        df.to_parquet(f)

        file_url = urlunparse(
            get_staging_client(staging_uri.scheme, config).upload_fileobj(
                f,
                f.name,
                remote_path_prefix=os.path.join(staging_location, "dataframes"),
                remote_path_suffix=".parquet",
            )
        )

    return FileSource(
        event_timestamp_column=event_timestamp_column,
        file_format=ParquetFormat(),
        file_url=file_url,
    )
