# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
import time
from datetime import datetime
from typing import List, Optional, cast

from azure.synapse.spark.models import SparkBatchJob
from azure.identity import DefaultAzureCredential, DeviceCodeCredential, ChainedTokenCredential, ManagedIdentityCredential,EnvironmentCredential

from feast_spark.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

from .synapse_utils import (
    HISTORICAL_RETRIEVAL_JOB_TYPE,
    LABEL_JOBTYPE,
    LABEL_FEATURE_TABLE,
    METADATA_JOBHASH,
    METADATA_OUTPUT_URI,
    OFFLINE_TO_ONLINE_JOB_TYPE,
    STREAM_TO_ONLINE_JOB_TYPE,
    SynapseJobRunner,
    DataLakeFiler,
    _prepare_job_tags,
    _job_feast_state,
    _job_start_time,
    _cancel_job_by_id,
    _get_job_by_id,
    _list_jobs,
    _submit_job,
)


class SynapseJobMixin:
    def __init__(self, api: SynapseJobRunner, job_id: int):
        self._api = api
        self._job_id = job_id

    def get_id(self) -> str:
        return self._job_id

    def get_status(self) -> SparkJobStatus:
        job = _get_job_by_id(self._api, self._job_id)
        assert job is not None
        return _job_feast_state(job)

    def get_start_time(self) -> datetime:
        job = _get_job_by_id(self._api, self._job_id)
        assert job is not None
        return _job_start_time(job)

    def cancel(self):
        _cancel_job_by_id(self._api, self._job_id)

    def _wait_for_complete(self, timeout_seconds: Optional[float]) -> bool:
        """ Returns true if the job completed successfully """
        start_time = time.time()
        while (timeout_seconds is None) or (time.time() - start_time < timeout_seconds):
            status = self.get_status()
            if status == SparkJobStatus.COMPLETED:
                return True
            elif status == SparkJobStatus.FAILED:
                return False
            else:
                time.sleep(1)
        else:
            raise TimeoutError("Timeout waiting for job to complete")


class SynapseRetrievalJob(SynapseJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a synapse cluster
    """

    def __init__(
        self, api: SynapseJobRunner, job_id: int, output_file_uri: str
    ):
        """
        This is the job object representing the historical retrieval job, returned by SynapseClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(api, job_id)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None, block=True):
        if not block:
            return self._output_file_uri

        if self._wait_for_complete(timeout_sec):
            return self._output_file_uri
        else:
            raise SparkJobFailure("Spark job failed")


class SynapseBatchIngestionJob(SynapseJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a synapse cluster
    """

    def __init__(
        self, api: SynapseJobRunner, job_id: int, feature_table: str
    ):
        super().__init__(api, job_id)
        self._feature_table = feature_table

    def get_feature_table(self) -> str:
        return self._feature_table


class SynapseStreamIngestionJob(SynapseJobMixin, StreamIngestionJob):
    """
    Ingestion streaming job for a synapse cluster
    """

    def __init__(
        self,
        api: SynapseJobRunner,
        job_id: int,
        job_hash: str,
        feature_table: str,
    ):
        super().__init__(api, job_id)
        self._job_hash = job_hash
        self._feature_table = feature_table

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._feature_table

login_credential_cache = None

class SynapseJobLauncher(JobLauncher):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    def __init__(
        self,
        synapse_dev_url: str,
        pool_name: str,
        datalake_dir: str,
        executor_size: str,
        executors: int
    ):
        tenant_id='72f988bf-86f1-41af-91ab-2d7cd011db47'
        authority_host_uri = 'login.microsoftonline.com'
        client_id = '04b07795-8ddb-461a-bbee-02f9e1bf7b46' 

        global login_credential_cache
        # use a global cache to store the credential, to avoid users from multiple login

        if login_credential_cache is None:
            # use DeviceCodeCredential if EnvironmentCredential is not available
            self.credential = ChainedTokenCredential(EnvironmentCredential(), DeviceCodeCredential(client_id, authority=authority_host_uri, tenant=tenant_id))
            login_credential_cache = self.credential
        else:
            self.credential = login_credential_cache

        self._api = SynapseJobRunner(synapse_dev_url, pool_name, executor_size = executor_size, executors = executors, credential=self.credential)
        self._datalake = DataLakeFiler(datalake_dir,credential=self.credential)

    def _job_from_job_info(self, job_info: SparkBatchJob) -> SparkJob:
        job_type = job_info.tags[LABEL_JOBTYPE]
        if job_type == HISTORICAL_RETRIEVAL_JOB_TYPE:
            assert METADATA_OUTPUT_URI in job_info.tags
            return SynapseRetrievalJob(
                api=self._api,
                job_id=job_info.id,
                output_file_uri=job_info.tags[METADATA_OUTPUT_URI],
            )
        elif job_type == OFFLINE_TO_ONLINE_JOB_TYPE:
            return SynapseBatchIngestionJob(
                api=self._api,
                job_id=job_info.id,
                feature_table=job_info.tags.get(LABEL_FEATURE_TABLE, ""),
            )
        elif job_type == STREAM_TO_ONLINE_JOB_TYPE:
            # job_hash must not be None for stream ingestion jobs
            assert METADATA_JOBHASH in job_info.tags
            return SynapseStreamIngestionJob(
                api=self._api,
                job_id=job_info.id,
                job_hash=job_info.tags[METADATA_JOBHASH],
                feature_table=job_info.tags.get(LABEL_FEATURE_TABLE, ""),
            )
        else:
            # We should never get here
            raise ValueError(f"Unknown job type {job_type}")

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            RetrievalJob: wrapper around remote job that returns file uri to the result file.
        """

        main_file = self._datalake.upload_file(job_params.get_main_file_path())
        job_info = _submit_job(self._api, "Historical-Retrieval", main_file,
                    arguments = job_params.get_arguments(),
                    tags = {LABEL_JOBTYPE: HISTORICAL_RETRIEVAL_JOB_TYPE,
                            METADATA_OUTPUT_URI: job_params.get_destination_path()})

        return cast(RetrievalJob, self._job_from_job_info(job_info))

    def offline_to_online_ingestion(
        self, ingestion_job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        """
        Submits a batch ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            BatchIngestionJob: wrapper around remote job that can be used to check when job completed.
        """

        main_file = self._datalake.upload_file(ingestion_job_params.get_main_file_path())

        job_info = _submit_job(self._api, ingestion_job_params.get_project()+"_offline_to_online_ingestion", main_file,
            main_class = ingestion_job_params.get_class_name(),
            arguments = ingestion_job_params.get_arguments(),
            reference_files=[main_file],
            tags = _prepare_job_tags(ingestion_job_params, OFFLINE_TO_ONLINE_JOB_TYPE),configuration=None)

        return cast(BatchIngestionJob, self._job_from_job_info(job_info))

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        """
        Starts a stream ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            StreamIngestionJob: wrapper around remote job.
        """

        main_file = self._datalake.upload_file(ingestion_job_params.get_main_file_path())

        extra_jar_paths: List[str] = []
        for extra_jar in ingestion_job_params.get_extra_jar_paths():
            extra_jar_paths.append(self._datalake.upload_file(extra_jar))

        tags = _prepare_job_tags(ingestion_job_params, STREAM_TO_ONLINE_JOB_TYPE)
        tags[METADATA_JOBHASH] = ingestion_job_params.get_job_hash()
        job_info = _submit_job(self._api, ingestion_job_params.get_project()+"_stream_to_online_ingestion", main_file,
            main_class = ingestion_job_params.get_class_name(),
            arguments = ingestion_job_params.get_arguments(),
            reference_files = extra_jar_paths,
            configuration=None,
            tags = tags)

        return cast(StreamIngestionJob, self._job_from_job_info(job_info))

    def get_job_by_id(self, job_id: int) -> SparkJob:
        job_info = _get_job_by_id(self._api, job_id)
        if job_info is None:
            raise KeyError(f"Job iwth id {job_id} not found")
        else:
            return self._job_from_job_info(job_info)

    def list_jobs(
        self,
        include_terminated: bool,
        project: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SparkJob]:
        return [
            self._job_from_job_info(job)
            for job in _list_jobs(self._api, project, table_name)
            if include_terminated
            or _job_feast_state(job) not in (SparkJobStatus.COMPLETED, SparkJobStatus.FAILED)
        ]
