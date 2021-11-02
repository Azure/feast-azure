import copy
import hashlib
import json
from datetime import datetime
from unittest.mock import Mock

import pytest

from feast import Client as FeastClient
from feast.config import Config
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.feature_table import FeatureTable
from feast_spark.client import Client
from feast_spark.job_service import ensure_stream_ingestion_jobs
from feast_spark.pyspark.abc import SparkJobStatus, StreamIngestionJob
from feast_spark.pyspark.launcher import _feature_table_to_argument, _source_to_argument


@pytest.fixture
def feast_client():
    c = FeastClient(
        job_service_pause_between_jobs=0,
        options={"whitelisted_projects": "default,ride"},
    )
    c.list_projects = Mock(return_value=["default", "ride", "invalid_project"])
    c.list_feature_tables = Mock()

    yield c


@pytest.fixture
def spark_client(feast_client):
    c = Client(feast_client)
    c.list_jobs = Mock()
    c.start_stream_to_online_ingestion = Mock()

    yield c


@pytest.fixture
def feature_table():
    return FeatureTable(
        name="ft",
        entities=[],
        features=[],
        stream_source=KafkaSource(
            topic="t",
            bootstrap_servers="",
            message_format=AvroFormat(""),
            event_timestamp_column="",
        ),
    )


class SimpleStreamingIngestionJob(StreamIngestionJob):
    def __init__(
        self, id: str, project: str, feature_table: FeatureTable, status: SparkJobStatus
    ):
        self._id = id
        self._feature_table = feature_table
        self._project = project
        self._status = status
        self._hash = hash

    def get_hash(self) -> str:
        source = _source_to_argument(self._feature_table.stream_source, Config())
        feature_table = _feature_table_to_argument(None, self._project, self._feature_table)  # type: ignore

        job_json = json.dumps(
            {"source": source, "feature_table": feature_table}, sort_keys=True,
        )
        return hashlib.md5(job_json.encode()).hexdigest()

    def get_feature_table(self) -> str:
        return self._feature_table.name

    def get_id(self) -> str:
        return self._id

    def get_status(self) -> SparkJobStatus:
        return self._status

    def cancel(self):
        self._status = SparkJobStatus.COMPLETED

    def get_start_time(self) -> datetime:
        pass


def test_new_job_creation(spark_client, feature_table):
    """ No job existed prior to call """

    spark_client.feature_store.list_feature_tables.return_value = [feature_table]
    spark_client.list_jobs.return_value = []

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert spark_client.start_stream_to_online_ingestion.call_count == 2


def test_no_changes(spark_client, feature_table):
    """ Feature Table spec is the same """

    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.IN_PROGRESS
    )
    job2 = SimpleStreamingIngestionJob(
        "", "ride", feature_table, SparkJobStatus.IN_PROGRESS
    )

    spark_client.feature_store.list_feature_tables.return_value = [feature_table]
    spark_client.list_jobs.return_value = [job, job2]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert job.get_status() == SparkJobStatus.IN_PROGRESS
    spark_client.start_stream_to_online_ingestion.assert_not_called()


def test_update_existing_job(spark_client, feature_table):
    """ Feature Table spec was updated """

    new_ft = copy.deepcopy(feature_table)
    new_ft.stream_source._kafka_options.topic = "new_t"
    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.IN_PROGRESS
    )

    spark_client.feature_store.list_feature_tables.return_value = [new_ft]
    spark_client.list_jobs.return_value = [job]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert job.get_status() == SparkJobStatus.COMPLETED
    assert spark_client.start_stream_to_online_ingestion.call_count == 2


def test_not_cancelling_starting_job(spark_client, feature_table):
    """ Feature Table spec was updated but previous version is still starting """

    new_ft = copy.deepcopy(feature_table)
    new_ft.stream_source._kafka_options.topic = "new_t"
    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.STARTING
    )

    spark_client.feature_store.list_feature_tables.return_value = [new_ft]
    spark_client.list_jobs.return_value = [job]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert job.get_status() == SparkJobStatus.STARTING
    assert spark_client.start_stream_to_online_ingestion.call_count == 2


def test_not_retrying_failed_job(spark_client, feature_table):
    """ Job has failed on previous try """

    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.FAILED
    )

    spark_client.feature_store.list_feature_tables.return_value = [feature_table]
    spark_client.list_jobs.return_value = [job]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    spark_client.list_jobs.assert_called_once_with(include_terminated=True)
    assert job.get_status() == SparkJobStatus.FAILED
    spark_client.start_stream_to_online_ingestion.assert_called_once_with(
        feature_table, [], project="ride"
    )


def test_restarting_completed_job(spark_client, feature_table):
    """ Job has succesfully finished on previous try """
    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.COMPLETED
    )

    spark_client.feature_store.list_feature_tables.return_value = [feature_table]
    spark_client.list_jobs.return_value = [job]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert spark_client.start_stream_to_online_ingestion.call_count == 2


def test_stopping_running_job(spark_client, feature_table):
    """ Streaming source was deleted """
    new_ft = copy.deepcopy(feature_table)
    new_ft.stream_source = None

    job = SimpleStreamingIngestionJob(
        "", "default", feature_table, SparkJobStatus.IN_PROGRESS
    )

    spark_client.feature_store.list_feature_tables.return_value = [new_ft]
    spark_client.list_jobs.return_value = [job]

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    assert job.get_status() == SparkJobStatus.COMPLETED
    spark_client.start_stream_to_online_ingestion.assert_not_called()


def test_restarting_failed_jobs(feature_table):
    """ If configured - restart failed jobs """

    feast_client = FeastClient(
        job_service_pause_between_jobs=0,
        job_service_retry_failed_jobs=True,
        options={"whitelisted_projects": "default,ride"},
    )
    feast_client.list_projects = Mock(return_value=["default"])
    feast_client.list_feature_tables = Mock()

    spark_client = Client(feast_client)
    spark_client.list_jobs = Mock()
    spark_client.start_stream_to_online_ingestion = Mock()

    spark_client.feature_store.list_feature_tables.return_value = [feature_table]
    spark_client.list_jobs.return_value = []

    ensure_stream_ingestion_jobs(spark_client, all_projects=True)

    spark_client.list_jobs.assert_called_once_with(include_terminated=False)
    spark_client.start_stream_to_online_ingestion.assert_called_once_with(
        feature_table, [], project="default"
    )
