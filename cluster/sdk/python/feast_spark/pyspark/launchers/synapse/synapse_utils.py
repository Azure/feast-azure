# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import re
import hashlib
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional
from azure.core.configuration import Configuration

from azure.identity import DefaultAzureCredential

from azure.synapse.spark import SparkClient 
from azure.synapse.spark.models import SparkBatchJobOptions, SparkBatchJob

from azure.storage.filedatalake import DataLakeServiceClient

from feast_spark.pyspark.abc import SparkJobStatus

__all__ = [
    "_cancel_job_by_id",
    "_prepare_job_tags",
    "_list_jobs",
    "_get_job_by_id",
    "_generate_project_table_hash",
    "STREAM_TO_ONLINE_JOB_TYPE",
    "OFFLINE_TO_ONLINE_JOB_TYPE",
    "HISTORICAL_RETRIEVAL_JOB_TYPE",
    "METADATA_JOBHASH",
    "METADATA_OUTPUT_URI",
]

STREAM_TO_ONLINE_JOB_TYPE = "STREAM_TO_ONLINE_JOB"
OFFLINE_TO_ONLINE_JOB_TYPE = "OFFLINE_TO_ONLINE_JOB"
HISTORICAL_RETRIEVAL_JOB_TYPE = "HISTORICAL_RETRIEVAL_JOB"

LABEL_JOBID = "feast.dev/jobid"
LABEL_JOBTYPE = "feast.dev/type"
LABEL_FEATURE_TABLE = "feast.dev/table"
LABEL_FEATURE_TABLE_HASH = "feast.dev/tablehash"
LABEL_PROJECT = "feast.dev/project"

# Can't store these bits of info due to 64-character limit, so we store them as
# sparkConf
METADATA_OUTPUT_URI = "dev.feast.outputuri"
METADATA_JOBHASH = "dev.feast.jobhash"


def _generate_project_table_hash(project: str, table_name: str) -> str:
    return hashlib.md5(f"{project}:{table_name}".encode()).hexdigest()


def _truncate_label(label: str) -> str:
    return label[:63]


def _prepare_job_tags(job_params, job_type: str) -> Dict[str, Any]:
    """ Prepare Synapse job tags """
    return {LABEL_JOBTYPE:job_type,
            LABEL_FEATURE_TABLE: _truncate_label(
                job_params.get_feature_table_name()
            ),
            LABEL_FEATURE_TABLE_HASH: _generate_project_table_hash(
                job_params.get_project(),
                job_params.get_feature_table_name(),
            ),
            LABEL_PROJECT: job_params.get_project()
        }


STATE_MAP = {
    "": SparkJobStatus.STARTING,
    "not_started": SparkJobStatus.STARTING,
    'starting': SparkJobStatus.STARTING,
    "running": SparkJobStatus.IN_PROGRESS,
    "success": SparkJobStatus.COMPLETED,
    "dead": SparkJobStatus.FAILED,
    "killed": SparkJobStatus.FAILED,
    "Uncertain": SparkJobStatus.IN_PROGRESS,
    "Succeeded": SparkJobStatus.COMPLETED,
    "Failed": SparkJobStatus.FAILED,
    "Cancelled": SparkJobStatus.FAILED,
}


def _job_feast_state(job: SparkBatchJob) -> SparkJobStatus:
    return STATE_MAP[job.state]


def _job_start_time(job: SparkBatchJob) -> datetime:
    return job.scheduler.scheduled_at


EXECUTOR_SIZE = {'Small': {'Cores': 4, 'Memory': '28g'}, 'Medium': {'Cores': 8, 'Memory': '56g'},
                 'Large': {'Cores': 16, 'Memory': '112g'}}


def categorized_files(reference_files):
    if reference_files == None:
        return None, None

    files = []
    jars = []
    for file in reference_files:
        file = file.strip()
        if file.endswith(".jar"):
            jars.append(file)
        else:
            files.append(file)
    return files, jars


class SynapseJobRunner(object):
    def __init__(self, synapse_dev_url, spark_pool_name, credential = None, executor_size = 'Small', executors = 2):
        if credential is None:
            credential = DefaultAzureCredential()

        self.client = SparkClient(
                credential=credential,
                endpoint=synapse_dev_url, 
                spark_pool_name=spark_pool_name 
            )

        self._executor_size = executor_size
        self._executors = executors

    def get_spark_batch_job(self, job_id):

        return self.client.spark_batch.get_spark_batch_job(job_id, detailed=True)

    def get_spark_batch_jobs(self):

        return self.client.spark_batch.get_spark_batch_jobs(detailed=True)

    def cancel_spark_batch_job(self, job_id):

        return self.client.spark_batch.cancel_spark_batch_job(job_id)

    def create_spark_batch_job(self, job_name, main_definition_file, class_name = None, 
                            arguments=None,  reference_files=None, archives=None, configuration=None, tags=None):

        file = main_definition_file

        files, jars = categorized_files(reference_files)
        driver_cores = EXECUTOR_SIZE[self._executor_size]['Cores']
        driver_memory = EXECUTOR_SIZE[self._executor_size]['Memory']
        executor_cores = EXECUTOR_SIZE[self._executor_size]['Cores']
        executor_memory = EXECUTOR_SIZE[self._executor_size]['Memory']

        # This is needed to correctly set the spark properties needed by org.apache.hadoop.fs.azure.NativeAzureFileSystem
        # Please see: https://github.com/Azure/feast-azure/issues/41
        if "FEAST_AZURE_BLOB_ACCOUNT_NAME" in os.environ and "FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY" in os.environ:
          blob_configuration = {f'spark.hadoop.fs.azure.account.key.{os.environ["FEAST_AZURE_BLOB_ACCOUNT_NAME"]}.blob.core.windows.net': os.environ["FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY"]}
          configuration = blob_configuration if configuration is None else configuration.update(blob_configuration)

        # SDK source code is here: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/synapse/azure-synapse
        # Exact code is here: https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/synapse/azure-synapse-spark/azure/synapse/spark/operations/_spark_batch_operations.py#L114
        # Adding spaces between brackets. This is to workaround this known YARN issue (when running Spark on YARN):
        # https://issues.apache.org/jira/browse/SPARK-17814?focusedCommentId=15567964&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-15567964
        # print(arguments)
        updated_arguments = []
        for elem in arguments:
            if type(elem) == str:
                updated_arguments.append(elem.replace("}", " }"))
            else:
                updated_arguments.append(elem)


        spark_batch_job_options = SparkBatchJobOptions(
            tags=tags,
            name=job_name,
            file=file,
            class_name=class_name,
            arguments=updated_arguments,
            jars=jars,
            files=files,
            archives=archives,
            configuration=configuration,
            driver_memory=driver_memory,
            driver_cores=driver_cores,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            executor_count=self._executors)

        return self.client.spark_batch.create_spark_batch_job(spark_batch_job_options, detailed=True)


class DataLakeFiler(object):
    def __init__(self, datalake_dir, credential = None):
        datalake = list(filter(None, re.split('/|@', datalake_dir)))
        assert len(datalake) >= 3
        
        if credential is None:
            credential = DefaultAzureCredential()

        account_url = "https://" + datalake[2]
        datalake_client = DataLakeServiceClient(
            credential=credential,
            account_url=account_url
        ).get_file_system_client(datalake[1])

        if len(datalake) > 3:
            datalake_client = datalake_client.get_directory_client('/'.join(datalake[3:]))
            datalake_client.create_directory()

        self.datalake_dir = datalake_dir + '/' if datalake_dir[-1] != '/' else datalake_dir
        self.dir_client = datalake_client 

    def upload_file(self, local_file):

        file_name = os.path.basename(local_file)
        file_client = self.dir_client.create_file(file_name)

        if local_file.startswith('http'):
            # remote_file = local_file
            # local_file = './' + file_name
            # urllib.request.urlretrieve(remote_file, local_file)
            with urllib.request.urlopen(local_file) as f:
                data = f.read()
                file_client.append_data(data, 0, len(data))
                file_client.flush_data(len(data))
        else:
            with open(local_file, 'r') as f:
                data = f.read()
                file_client.append_data(data, 0, len(data))
                file_client.flush_data(len(data))
            
        return self.datalake_dir + file_name


def _submit_job(
    api: SynapseJobRunner,
    name: str,
    main_file: str,
    main_class = None,
    arguments = None,
    reference_files = None,
    tags = None,
    configuration = None,
) -> SparkBatchJob:
    return api.create_spark_batch_job(name, main_file, class_name = main_class, arguments = arguments,
                                      reference_files = reference_files, tags = tags, configuration=configuration)


def _list_jobs(
    api: SynapseJobRunner,
    project: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[SparkBatchJob]:

    job_infos = api.get_spark_batch_jobs()

    # Batch, Streaming Ingestion jobs
    if project and table_name:
        result = []
        table_name_hash = _generate_project_table_hash(project, table_name)
        for job_info in job_infos:
            if LABEL_FEATURE_TABLE_HASH in job_info.tags:
                if table_name_hash == job_info.tags[LABEL_FEATURE_TABLE_HASH]:
                    result.append(job_info)
    elif project:
        result = []
        for job_info in job_infos:
            if LABEL_PROJECT in job_info.tags:
                if project == job_info.tags[LABEL_PROJECT]:
                    result.append(job_info)
    else:
        result = job_infos

    return result


def _get_job_by_id(
    api: SynapseJobRunner,
    job_id: int
) -> Optional[SparkBatchJob]:
    return api.get_spark_batch_job(job_id)


def _cancel_job_by_id(api: SynapseJobRunner, job_id: int):
    api.cancel_spark_batch_job(job_id)

