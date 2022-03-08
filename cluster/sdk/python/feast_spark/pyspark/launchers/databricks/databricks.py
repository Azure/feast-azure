import json
import random
import string
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from io import BytesIO
from urllib.parse import urlunparse, urlparse

import requests
import yaml
from feast.staging.storage_client import AbstractStagingClient, get_staging_client

from feast_spark.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    ScheduledBatchIngestionJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

from .databricks_utils import (
    HISTORICAL_RETRIEVAL_JOB_TYPE,
    LABEL_FEATURE_TABLE,
    METADATA_JOBHASH,
    METADATA_OUTPUT_URI,
    OFFLINE_TO_ONLINE_JOB_TYPE,
    STREAM_TO_ONLINE_JOB_TYPE,
    _cancel_job_by_id,
    _get_job_by_id,
    _list_jobs,
    _submit_job, DatabricksJobManager, DatabricksJobInfo, HISTORICAL_RETRIEVAL_JOB_TYPE_CODE,
    get_job_metadata, OFFLINE_TO_ONLINE_JOB_TYPE_CODE, _generate_job_extra_metadata, STREAM_TO_ONLINE_JOB_TYPE_CODE,
    b64_encode,
)


def _load_resource_template(job_template_path: Path) -> Dict[str, Any]:
    with open(job_template_path, "rt") as f:
        return yaml.safe_load(f)


def _generate_job_id() -> str:
    return "feast-" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
    )


def _generate_scheduled_job_id(project: str, feature_table_name: str) -> str:
    scheduled_job_id = f"feast-{project}-{feature_table_name}".replace("_", "-")
    k8s_res_name_char_limit = 253
    
    return (
        scheduled_job_id
        if len(scheduled_job_id) <= k8s_res_name_char_limit
        else scheduled_job_id[:k8s_res_name_char_limit]
    )


def _truncate_label(label: str) -> str:
    return label[:63]


class DatabricksJobMixin:
    def __init__(self, api: DatabricksJobManager, job_id: int):
        self._api = api
        self._job_id = job_id
    
    def get_id(self) -> int:
        return self._job_id
    
    def get_status(self) -> SparkJobStatus:
        job = _get_job_by_id(self._api, self._job_id)
        assert job is not None
        return job.state
    
    def get_start_time(self) -> datetime:
        job = _get_job_by_id(self._api, self._job_id)
        assert job is not None
        return job.start_time
    
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


class DatabricksRetrievalJob(DatabricksJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a synapse cluster
    """
    
    def __init__(
            self, api: DatabricksJobManager, job_id: int, output_file_uri: str
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


class DatabricksBatchIngestionJob(DatabricksJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a synapse cluster
    """
    
    def __init__(
            self, api: DatabricksJobManager, job_id: int, feature_table: str
    ):
        super().__init__(api, job_id)
        self._feature_table = feature_table
    
    def get_feature_table(self) -> str:
        return self._feature_table


class DatabricksStreamIngestionJob(DatabricksJobMixin, StreamIngestionJob):
    """
    Ingestion streaming job for a synapse cluster
    """
    
    def __init__(
            self,
            api: DatabricksJobManager,
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


class DatabricksJobLauncher(JobLauncher):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    Databricks Job Launcher supports mounted storage accounts as well
    """
    
    def __init__(
            self,
            databricks_access_token: str,
            databricks_host_url: str,
            databricks_common_cluster_id: str,
            staging_client: AbstractStagingClient,
            databricks_streaming_cluster_id: Optional[str] = None,
            databricks_max_active_jobs_to_retrieve: Optional[int] = 5000,
            mounted_staging_location: Optional[str] = None,
            azure_account_key: Optional[str] = None,
            staging_location: Optional[str] = None,
            azure_account_name: Optional[str] = None,
    ):
        if mounted_staging_location is None and (azure_account_key is None or staging_location is None or
                                                 azure_account_name is None):
            raise Exception("Error: Storage path unavailable, Please add mounted storage location or "
                            "remote azure storage location")
        self._staging_location = staging_location
        self._storage_client = staging_client
        self._azure_account_name = azure_account_name
        self._azure_account_key = azure_account_key
        
        self._mounted_staging_location = mounted_staging_location + '/' if mounted_staging_location[
                                                                               -1] != '/' else mounted_staging_location
        if self._mounted_staging_location.startswith("/dbfs/"):
            self._mounted_staging_location = "dbfs:" + self._mounted_staging_location[5]
        
        self._api = DatabricksJobManager(databricks_access_token=databricks_access_token,
                                         databricks_host_url=databricks_host_url,
                                         cluster_id=databricks_common_cluster_id,
                                         streaming_cluster_id=databricks_streaming_cluster_id,
                                         max_active_jobs_to_retrieve=databricks_max_active_jobs_to_retrieve)
    
    def _job_from_job_info(self, job_info: DatabricksJobInfo) -> SparkJob:
        job_type, job_extra_metadata = get_job_metadata(job_info)
        if job_type == HISTORICAL_RETRIEVAL_JOB_TYPE:
            assert METADATA_OUTPUT_URI in job_extra_metadata
            return DatabricksRetrievalJob(
                api=self._api,
                job_id=job_info.job_id,
                output_file_uri=job_extra_metadata[METADATA_OUTPUT_URI],
            )
        elif job_type == OFFLINE_TO_ONLINE_JOB_TYPE:
            return DatabricksBatchIngestionJob(
                api=self._api,
                job_id=job_info.job_id,
                feature_table=job_extra_metadata.get(LABEL_FEATURE_TABLE, ""),
            )
        elif job_type == STREAM_TO_ONLINE_JOB_TYPE:
            # job_hash must not be None for stream ingestion jobs
            assert METADATA_JOBHASH in job_extra_metadata
            return DatabricksStreamIngestionJob(
                api=self._api,
                job_id=job_info.job_id,
                job_hash=job_extra_metadata[METADATA_JOBHASH],
                feature_table=job_extra_metadata.get(LABEL_FEATURE_TABLE, ""),
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
        with open(job_params.get_main_file_path()) as f:
            pyspark_script = f.read()
        
        pyspark_script_path = urlunparse(
            self._storage_client.upload_fileobj(
                BytesIO(pyspark_script.encode("utf8")),
                local_path="historical_retrieval.py",
                remote_path_prefix=self._staging_location,
                remote_path_suffix=".py",
            )
        )
        relative_path = str(pyspark_script_path).rsplit("/", 1)[1] if not pyspark_script_path.endswith("/") else str(
            pyspark_script_path).rsplit("/", 2)[1]
        
        dbfs_mounted_path = self._mounted_staging_location + relative_path
        job_args = job_params.get_arguments()
        job_args.extend(["--mounted_staging_location", b64_encode(self._mounted_staging_location)])
        spark_python_task_info = {
            "python_file": dbfs_mounted_path,
            "parameters": job_args
        }
        job_extra_metadata = {METADATA_OUTPUT_URI: job_params.get_destination_path()}
        job_info = _submit_job(self._api, HISTORICAL_RETRIEVAL_JOB_TYPE_CODE, job_extra_metadata,
                               spark_task_type="spark_python_task", spark_task_info=spark_python_task_info)
        
        return cast(RetrievalJob, self._job_from_job_info(job_info))
    
    def _upload_jar(self, jar_path: str, jar_name: str) -> str:
        if jar_path.startswith("dbfs:/"):
            return jar_path
        elif (
                jar_path.startswith("s3://")
                or jar_path.startswith("s3a://")
                or jar_path.startswith("https://")
                or jar_path.startswith("local://")
                or jar_path.startswith("wasbs://")
        ):
            r = requests.get(jar_path, allow_redirects=True)
            
            jar_path = urlunparse(self._storage_client.upload_fileobj(
                BytesIO(r.content),
                jar_name,
                remote_path_prefix=self._staging_location,
                remote_path_suffix=".jar",
            ))
            return jar_path
        elif jar_path.startswith("file://"):
            local_jar_path = urlparse(jar_path).path
        else:
            local_jar_path = jar_path
        with open(local_jar_path, "rb") as f:
            return urlunparse(
                self._storage_client.upload_fileobj(
                    f,
                    local_jar_path,
                    remote_path_prefix=self._staging_location,
                    remote_path_suffix=".jar",
                )
            )
    
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
        print(ingestion_job_params.get_main_file_path())
        
        libraries = [
            {
                "jar": self._upload_jar(ingestion_job_params.get_main_file_path(), "feast-ingestion-spark-develop.jar")
            }
        ]
        arguments = ingestion_job_params.get_arguments() + ["--databricks-runtime"]
        # arguments = [argument if argument.startswith("--") else b64_encode(argument) for argument in arguments]
        spark_jar_task_info = {
            "main_class_name": ingestion_job_params.get_class_name(),
            "parameters": arguments
        }
        
        job_info = _submit_job(self._api, OFFLINE_TO_ONLINE_JOB_TYPE_CODE,
                               _generate_job_extra_metadata(ingestion_job_params), spark_task_type="spark_jar_task",
                               spark_task_info=spark_jar_task_info, libraries=libraries)
        
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
        
        jars = [{
            "jar": self._upload_jar(ingestion_job_params.get_main_file_path(), "feast-ingestion-spark-develop.jar")
        }]
        for extra_jar in ingestion_job_params.get_extra_jar_paths():
            jar_path = self._upload_jar(extra_jar, "feast-ingestion-spark-develop.jar")
            relative_path = jar_path.rsplit("/", 1)[1] if not jar_path.endswith("/") else jar_path.rsplit("/", 2)[1]
            jars.append(
                {
                    "jar": "dbfs:" + self._mounted_staging_location + relative_path
                }
            )
        
        arguments = ingestion_job_params.get_arguments() + ["--databricks-runtime"]
        # arguments = [argument if argument.startswith("--") else b64_encode(argument) for argument in arguments]
        spark_jar_task_info = {
            "main_class_name": ingestion_job_params.get_class_name(),
            "parameters": arguments
        }
        
        job_info = _submit_job(self._api, STREAM_TO_ONLINE_JOB_TYPE_CODE,
                               _generate_job_extra_metadata(ingestion_job_params), spark_task_type="spark_jar_task",
                               spark_task_info=spark_jar_task_info, libraries=jars, use_stream_cluster=True)
        
        return cast(StreamIngestionJob, self._job_from_job_info(job_info))
    
    def get_job_by_id(self, job_id: int) -> SparkJob:
        job_info = _get_job_by_id(self._api, job_id)
        if job_info is None:
            raise KeyError(f"Job with id {job_id} not found")
        else:
            return self._job_from_job_info(job_info)
    
    def list_jobs(
            self,
            include_terminated: bool,
            project: Optional[str] = None,
            table_name: Optional[str] = None,
    ) -> List[DatabricksJobInfo]:
        return _list_jobs(self._api, include_terminated, project, table_name)
    
    def schedule_offline_to_online_ingestion(
            self, ingestion_job_params: ScheduledBatchIngestionJobParameters
    ):
        raise NotImplementedError(
            "Schedule spark jobs are not supported by Databricks launcher"
        )
    
    def unschedule_offline_to_online_ingestion(self, project: str, feature_table: str):
        raise NotImplementedError(
            "Unschedule spark jobs are not supported by Databricks launcher"
        )
