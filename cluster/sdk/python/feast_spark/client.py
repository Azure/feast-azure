import configparser
import os
import uuid
from datetime import datetime
from itertools import groupby
from typing import List, Optional, Union, cast

import pandas as pd
from croniter import croniter

import feast
from feast.config import Config
from feast.constants import ConfigOptions as feast_opt
from feast.data_source import BigQuerySource, FileSource
from feast.grpc.grpc import create_grpc_channel
from feast.staging.entities import (
    stage_entities_to_bq,
    stage_entities_to_fs,
    table_reference_from_string,
)
from feast_spark.api.JobService_pb2 import (
    GetHistoricalFeaturesRequest,
    GetJobRequest,
    ListJobsRequest,
    ScheduleOfflineToOnlineIngestionJobRequest,
    StartOfflineToOnlineIngestionJobRequest,
    StartStreamToOnlineIngestionJobRequest,
    UnscheduleOfflineToOnlineIngestionJobRequest,
)
from feast_spark.api.JobService_pb2_grpc import JobServiceStub
from feast_spark.constants import ConfigOptions as opt
from feast_spark.pyspark.abc import RetrievalJob, SparkJob
from feast_spark.pyspark.launcher import (
    get_job_by_id,
    list_jobs,
    schedule_offline_to_online_ingestion,
    start_historical_feature_retrieval_job,
    start_historical_feature_retrieval_spark_session,
    start_offline_to_online_ingestion,
    start_stream_to_online_ingestion,
    unschedule_offline_to_online_ingestion,
)
from feast_spark.remote_job import (
    RemoteBatchIngestionJob,
    RemoteRetrievalJob,
    RemoteStreamIngestionJob,
    get_remote_job_from_proto,
)


class Client:
    _feast: feast.Client

    def __init__(self, feast_client: feast.Client):
        feast_client._config._config.read_dict(
            {configparser.DEFAULTSECT: opt().defaults()}
        )

        self._feast = feast_client
        self._job_service_stub: Optional[JobServiceStub] = None

    @property
    def config(self) -> Config:
        return self._feast._config

    @property
    def _extra_grpc_params(self):
        return self._feast._extra_grpc_params

    @property
    def feature_store(self) -> feast.Client:
        return self._feast

    @property
    def _use_job_service(self) -> bool:
        return self.config.exists(opt.JOB_SERVICE_URL)

    @property
    def _job_service(self):
        """
        Creates or returns the gRPC Feast Job Service Stub

        Returns: JobServiceStub
        """
        # Don't try to initialize job service stub if the job service is disabled
        if not self._use_job_service:
            return None

        if not self._job_service_stub:
            channel = create_grpc_channel(
                url=self.config.get(opt.JOB_SERVICE_URL),
                enable_ssl=self.config.getboolean(opt.JOB_SERVICE_ENABLE_SSL),
                enable_auth=self.config.getboolean(feast_opt.ENABLE_AUTH),
                ssl_server_cert_path=self.config.get(opt.JOB_SERVICE_SERVER_SSL_CERT),
                auth_metadata_plugin=self._feast._auth_metadata,
                timeout=self.config.getint(feast_opt.GRPC_CONNECTION_TIMEOUT),
            )
            self._job_service_service_stub = JobServiceStub(channel)
        return self._job_service_service_stub

    def get_historical_features(
        self,
        feature_refs: List[str],
        entity_source: Union[pd.DataFrame, FileSource, BigQuerySource],
        output_location: Optional[str] = None,
    ) -> RetrievalJob:
        """
        Launch a historical feature retrieval job.

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
            entity_source (Union[pd.DataFrame, FileSource, BigQuerySource]): Source for the entity rows.
                If entity_source is a Panda DataFrame, the dataframe will be staged
                to become accessible by spark workers.
                If one of feature tables' source is in BigQuery - entities will be upload to BQ.
                Otherwise to remote file storage (derived from configured staging location).
                It is also assumed that the column event_timestamp is present
                in the dataframe, and is of type datetime without timezone information.

                The user needs to make sure that the source (or staging location, if entity_source is
                a Panda DataFrame) is accessible from the Spark cluster that will be used for the
                retrieval job.
            output_location: Specifies the path in a bucket to write the exported feature data files

        Returns:
                Returns a retrieval job object that can be used to monitor retrieval
                progress asynchronously, and can be used to materialize the
                results.

        Examples:
            >>> import feast
            >>> import feast_spark
            >>> from feast.data_format import ParquetFormat
            >>> from datetime import datetime
            >>> feast_client = feast.Client(core_url="localhost:6565")
            >>> feature_refs = ["bookings:bookings_7d", "bookings:booking_14d"]
            >>> entity_source = FileSource("event_timestamp", ParquetFormat(), "gs://some-bucket/customer")
            >>> feature_retrieval_job = feast_spark.Client(feast_client).get_historical_features(
            >>>     feature_refs, entity_source)
            >>> output_file_uri = feature_retrieval_job.get_output_file_uri()
                "gs://some-bucket/output/
        """
        feature_tables = self._get_feature_tables_from_feature_refs(
            feature_refs, self._feast.project
        )

        assert all(ft.batch_source.created_timestamp_column for ft in feature_tables), (
            "All BatchSources attached to retrieved FeatureTables "
            "must have specified `created_timestamp_column` to be used in "
            "historical dataset generation."
        )

        if output_location is None:
            output_location = os.path.join(
                self.config.get(opt.HISTORICAL_FEATURE_OUTPUT_LOCATION),
                str(uuid.uuid4()),
            )
        output_format = self.config.get(opt.HISTORICAL_FEATURE_OUTPUT_FORMAT)
        feature_sources = [
            feature_table.batch_source for feature_table in feature_tables
        ]

        if isinstance(entity_source, pd.DataFrame):
            if any(isinstance(source, BigQuerySource) for source in feature_sources):
                if self.config.exists(opt.BQ_STAGING_PROJECT) and self.config.exists(
                    opt.BQ_STAGING_DATASET
                ):
                    staging_bq_project = self.config.get(opt.BQ_STAGING_PROJECT)
                    staging_bq_dataset = self.config.get(opt.BQ_STAGING_DATASET)
                else:
                    first_bq_source = [
                        source
                        for source in feature_sources
                        if isinstance(source, BigQuerySource)
                    ][0]
                    source_ref = table_reference_from_string(
                        first_bq_source.bigquery_options.table_ref
                    )
                    staging_bq_project = source_ref.project
                    staging_bq_dataset = source_ref.dataset_id

                entity_source = stage_entities_to_bq(
                    entity_source, staging_bq_project, staging_bq_dataset
                )
            else:
                entity_source = stage_entities_to_fs(
                    entity_source,
                    staging_location=self.config.get(opt.SPARK_STAGING_LOCATION),
                    config=self.config,
                )

        if self._use_job_service:
            response = self._job_service.GetHistoricalFeatures(
                GetHistoricalFeaturesRequest(
                    feature_refs=feature_refs,
                    entity_source=entity_source.to_proto(),
                    project=self._feast.project,
                    output_format=output_format,
                    output_location=output_location,
                ),
                **self._feast._extra_grpc_params(),
            )
            return RemoteRetrievalJob(
                self._job_service,
                self._extra_grpc_params,
                response.id,
                output_file_uri=response.output_file_uri,
                start_time=response.job_start_time.ToDatetime(),
                log_uri=response.log_uri,
            )
        else:
            return start_historical_feature_retrieval_job(
                client=self,
                project=self._feast.project,
                entity_source=entity_source,
                feature_tables=feature_tables,
                output_format=output_format,
                output_path=output_location,
            )

    def get_historical_features_df(
        self, feature_refs: List[str], entity_source: Union[FileSource, BigQuerySource],
    ):
        """
        Launch a historical feature retrieval job.

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
            entity_source (Union[FileSource, BigQuerySource]): Source for the entity rows.
                The user needs to make sure that the source is accessible from the Spark cluster
                that will be used for the retrieval job.

        Returns:
                Returns the historical feature retrieval result in the form of Spark dataframe.

        Examples:
            >>> import feast
            >>> import feast_spark
            >>> from feast.data_format import ParquetFormat
            >>> from datetime import datetime
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.getOrCreate()
            >>> feast_client = feast.Client(core_url="localhost:6565")
            >>> feature_refs = ["bookings:bookings_7d", "bookings:booking_14d"]
            >>> entity_source = FileSource("event_timestamp", ParquetFormat, "gs://some-bucket/customer")
            >>> df = feast_spark.Client(feast_client).get_historical_features(
            >>>     feature_refs, entity_source)
        """
        feature_tables = self._get_feature_tables_from_feature_refs(
            feature_refs, self._feast.project
        )
        return start_historical_feature_retrieval_spark_session(
            client=self,
            project=self._feast.project,
            entity_source=entity_source,
            feature_tables=feature_tables,
        )

    def _get_feature_tables_from_feature_refs(
        self, feature_refs: List[str], project: Optional[str]
    ):
        feature_refs_grouped_by_table = [
            (feature_table_name, list(grouped_feature_refs))
            for feature_table_name, grouped_feature_refs in groupby(
                feature_refs, lambda x: x.split(":")[0]
            )
        ]

        feature_tables = []
        for feature_table_name, grouped_feature_refs in feature_refs_grouped_by_table:
            feature_table = self._feast.get_feature_table(feature_table_name, project)
            feature_names = [f.split(":")[-1] for f in grouped_feature_refs]
            feature_table.features = [
                f for f in feature_table.features if f.name in feature_names
            ]
            feature_tables.append(feature_table)
        return feature_tables

    def start_offline_to_online_ingestion(
        self, feature_table: feast.FeatureTable, start: datetime, end: datetime,
    ) -> SparkJob:
        """
        Launch Ingestion Job from Batch Source to Online Store for given feature table

        Args:
            feature_table:  FeatureTable that will be ingested into the online store
            start: lower datetime boundary on which to filter the source
            end: upper datetime boundary on which to filter the source

        Returns: Spark Job Proxy object
        """
        if not self._use_job_service:
            return start_offline_to_online_ingestion(
                client=self,
                project=self._feast.project,
                feature_table=feature_table,
                start=start,
                end=end,
            )
        else:
            request = StartOfflineToOnlineIngestionJobRequest(
                project=self._feast.project, table_name=feature_table.name,
            )
            request.start_date.FromDatetime(start)
            request.end_date.FromDatetime(end)
            response = self._job_service.StartOfflineToOnlineIngestionJob(request)
            return RemoteBatchIngestionJob(
                self._job_service,
                self._extra_grpc_params,
                response.id,
                feature_table.name,
                response.job_start_time.ToDatetime(),
                response.log_uri,
            )

    def schedule_offline_to_online_ingestion(
        self,
        feature_table: feast.FeatureTable,
        ingestion_timespan: int,
        cron_schedule: str,
    ):
        """
        Launch Scheduled Ingestion Job from Batch Source to Online Store for given feature table

        Args:
            feature_table:  FeatureTable that will be ingested into the online store
            ingestion_timespan: Days of data which will be ingestion per job. The boundaries
                on which to filter the source are [end of day of execution date - ingestion_timespan (days) ,
                end of day of execution date)
            cron_schedule: Cron schedule expression

        Returns: Spark Job Proxy object
        """
        if not croniter.is_valid(cron_schedule):
            raise RuntimeError(f"{cron_schedule} is not a valid cron expression")
        if not self._use_job_service:
            schedule_offline_to_online_ingestion(
                client=self,
                project=self._feast.project,
                feature_table=feature_table,
                ingestion_timespan=ingestion_timespan,
                cron_schedule=cron_schedule,
            )
        else:
            request = ScheduleOfflineToOnlineIngestionJobRequest(
                project=self._feast.project,
                table_name=feature_table.name,
                ingestion_timespan=ingestion_timespan,
                cron_schedule=cron_schedule,
            )
            self._job_service.ScheduleOfflineToOnlineIngestionJob(request)

    def unschedule_offline_to_online_ingestion(
        self, feature_table: feast.FeatureTable, project=None
    ):
        feature_table_project = self._feast.project if project is None else project

        if not self._use_job_service:
            unschedule_offline_to_online_ingestion(
                client=self,
                project=feature_table_project,
                feature_table=feature_table.name,
            )
        else:
            request = UnscheduleOfflineToOnlineIngestionJobRequest(
                project=feature_table_project, table_name=feature_table.name,
            )

            self._job_service.UnscheduleOfflineToOnlineIngestionJob(request)

    def start_stream_to_online_ingestion(
        self,
        feature_table: feast.FeatureTable,
        extra_jars: Optional[List[str]] = None,
        project: str = None,
    ) -> SparkJob:
        if not self._use_job_service:
            return start_stream_to_online_ingestion(
                client=self,
                project=project or self._feast.project,
                feature_table=feature_table,
                extra_jars=extra_jars or [],
            )
        else:
            request = StartStreamToOnlineIngestionJobRequest(
                project=self._feast.project, table_name=feature_table.name,
            )
            response = self._job_service.StartStreamToOnlineIngestionJob(request)
            return RemoteStreamIngestionJob(
                self._job_service,
                self._extra_grpc_params,
                response.id,
                feature_table.name,
                response.job_start_time,
                response.log_uri,
            )

    def list_jobs(
        self,
        include_terminated: bool,
        project: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SparkJob]:
        """
        List ingestion jobs currently running in Feast.

        Args:
            include_terminated: Flag to include terminated jobs or not
            project: Optionally specify the project to use as filter when retrieving jobs
            table_name: Optionally specify name of feature table to use as filter when retrieving jobs
        Returns:
            List of SparkJob ingestion jobs.
        """
        if not self._use_job_service:
            return list_jobs(include_terminated, self, project, table_name)
        else:
            request = ListJobsRequest(
                include_terminated=include_terminated,
                project=cast(str, project),
                table_name=cast(str, table_name),
            )
            response = self._job_service.ListJobs(request)
            return [
                get_remote_job_from_proto(
                    self._job_service, self._feast._extra_grpc_params, job
                )
                for job in response.jobs
            ]

    def get_job_by_id(self, job_id: str) -> SparkJob:
        if not self._use_job_service:
            return get_job_by_id(job_id, self)
        else:
            request = GetJobRequest(job_id=job_id)
            response = self._job_service.GetJob(request)
            return get_remote_job_from_proto(
                self._job_service, self._feast._extra_grpc_params, response.job
            )
