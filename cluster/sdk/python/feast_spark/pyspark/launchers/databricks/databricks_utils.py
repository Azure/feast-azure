import hashlib
import json
import random
import string
from base64 import b64encode, b64decode
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional

from kubernetes import client

from kubernetes.client import ApiException

from feast_spark.pyspark.abc import SparkJobStatus
from .databricks_api_wrapper import DatabricksAPIWrapper

__all__ = [
    "_cancel_job_by_id",
    "_list_jobs",
    "_get_job_by_id",
    "_generate_project_table_hash",
    "_generate_job_extra_metadata",
    "get_job_metadata",
    "DatabricksJobManager",
    "LABEL_FEATURE_TABLE",
    "_submit_job",
    "STREAM_TO_ONLINE_JOB_TYPE",
    "OFFLINE_TO_ONLINE_JOB_TYPE",
    "HISTORICAL_RETRIEVAL_JOB_TYPE",
    "METADATA_JOBHASH",
    "METADATA_OUTPUT_URI",
    "DatabricksJobInfo",
    "STREAM_TO_ONLINE_JOB_TYPE_CODE",
    "OFFLINE_TO_ONLINE_JOB_TYPE_CODE",
    "HISTORICAL_RETRIEVAL_JOB_TYPE_CODE",
    "b64_encode"
]

STREAM_TO_ONLINE_JOB_TYPE = "STREAM_TO_ONLINE_JOB"
OFFLINE_TO_ONLINE_JOB_TYPE = "OFFLINE_TO_ONLINE_JOB"
HISTORICAL_RETRIEVAL_JOB_TYPE = "HISTORICAL_RETRIEVAL_JOB"

STREAM_TO_ONLINE_JOB_TYPE_CODE = "S2O"
OFFLINE_TO_ONLINE_JOB_TYPE_CODE = "O2O"
HISTORICAL_RETRIEVAL_JOB_TYPE_CODE = "HR"

LABEL_JOBID = "feast.dev/jobid"
LABEL_JOBTYPE = "feast.dev/type"
LABEL_FEATURE_TABLE = "feast.dev/table"
LABEL_FEATURE_TABLE_HASH = "feast.dev/tablehash"
LABEL_PROJECT = "feast.dev/project"

# Can't store these bits of info in k8s labels due to 64-character limit, so we store them as
# sparkConf
METADATA_OUTPUT_URI = "dev.feast.outputuri"
METADATA_JOBHASH = "dev.feast.jobhash"

METADATA_KEYS = set((METADATA_JOBHASH, METADATA_OUTPUT_URI))


def json_b64_decode(s: str) -> Any:
    return json.loads(b64decode(s.encode("ascii")))


def json_b64_encode(obj: dict) -> str:
    return b64encode(json.dumps(obj).encode("utf8")).decode("ascii")


def b64_encode(obj: str) -> str:
    return b64encode(obj.encode("ascii")).decode("ascii")


def generate_short_id(length: int = 6):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(length))


def _generate_project_table_hash(project: str, table_name: str) -> str:
    return hashlib.md5(f"{project}:{table_name}".encode()).hexdigest()


def _job_id_to_resource_name(job_id: str) -> str:
    return job_id


def _truncate_label(label: str) -> str:
    return label[:63]


def _prepare_job_name(job_type: str, extra_info: dict) -> str:
    return f"{job_type}_{json_b64_encode(extra_info)}_{generate_short_id()}"


class DatabricksJobInfo(NamedTuple):
    job_id: int
    run_id: int
    state: SparkJobStatus
    start_time: datetime
    job_name: str


STATE_MAP = {
    "": SparkJobStatus.STARTING,
    "PENDING": SparkJobStatus.STARTING,
    "RUNNING": SparkJobStatus.IN_PROGRESS,
    "TERMINATING": SparkJobStatus.IN_PROGRESS,
    "TERMINATED": SparkJobStatus.IN_PROGRESS,
    "SUCCESS": SparkJobStatus.COMPLETED,
    "FAILED": SparkJobStatus.FAILED,
    "TIMEDOUT": SparkJobStatus.FAILED,
    "CANCELED": SparkJobStatus.FAILED,
    "SKIPPED": SparkJobStatus.FAILED,
    "INTERNAL_ERROR": SparkJobStatus.FAILED,
}

TYPE_MAP = {
    "S2O": STREAM_TO_ONLINE_JOB_TYPE,
    "O2O": OFFLINE_TO_ONLINE_JOB_TYPE,
    "HR": HISTORICAL_RETRIEVAL_JOB_TYPE
}


# Fetch Jobtype, and extra meta info
def get_job_metadata(job: DatabricksJobInfo) -> (str, Any):
    job_metadata = job.job_name.split("_")
    return TYPE_MAP[job_metadata[0]], json_b64_decode(job_metadata[1])


def _generate_job_extra_metadata(job_params) -> Dict[str, Any]:
    return {
        LABEL_FEATURE_TABLE: _truncate_label(
            job_params.get_feature_table_name()
        ),
        LABEL_FEATURE_TABLE_HASH: _generate_project_table_hash(
            job_params.get_project(),
            job_params.get_feature_table_name(),
        ),
        LABEL_PROJECT: job_params.get_project()
    }


def _get_feast_job_state(job: DatabricksJobInfo) -> SparkJobStatus:
    return STATE_MAP[job.state]


def _get_job_start_time(job: DatabricksJobInfo) -> datetime:
    return job.start_time


def categorized_files(reference_files):
    if reference_files is None:
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


class DatabricksJobManager(object):
    def __init__(self, databricks_access_token: str, databricks_host_url: str, cluster_id: str,
                 streaming_cluster_id: Optional[str] = None, max_active_jobs_to_retrieve: int = 5000):
        self.client = DatabricksJobsClient(databricks_access_token=databricks_access_token,
                                           databricks_host_url=databricks_host_url)
        self.cluster_id = cluster_id
        self.streaming_cluster_id = streaming_cluster_id
        self.max_active_jobs_to_retrieve = max_active_jobs_to_retrieve
    
    def get_spark_job(self, job_id):
        return self.client.get_databricks_job(job_id)
    
    def get_all_spark_jobs(self, include_terminated: bool = False, project: Optional[str] = None,
                           table_name: Optional[str] = None) -> List[DatabricksJobInfo]:
        jobs = self.client.get_active_jobs(self.max_active_jobs_to_retrieve, self.cluster_id, include_terminated)
        if self.streaming_cluster_id is not None:
            jobs.extend(self.client.get_active_jobs(self.max_active_jobs_to_retrieve, self.streaming_cluster_id,
                                                    include_terminated))
        filtered_jobs = []
        for job in jobs:
            _, job_extra_metadata = get_job_metadata(job)
            if project is not None and job_extra_metadata[LABEL_PROJECT] == project:
                filtered_jobs.append(job)
            
            if table_name is not None and job_extra_metadata[LABEL_FEATURE_TABLE] == table_name:
                filtered_jobs.append(job)
        return filtered_jobs
    
    def cancel_spark_job(self, job_id):
        return self.client.cancel_job_run(job_id)
    
    def create_spark_job(self, job_type: str, job_extra_info: dict, spark_task_type: str, spark_task_info: dict,
                         libraries: list = None, use_stream_cluster: bool = False) -> DatabricksJobInfo:
        job_name = _prepare_job_name(job_type, job_extra_info)
        if use_stream_cluster and self.streaming_cluster_id is not None:
            return self.client.create_and_run_one_time_job(job_name=job_name, spark_task_type=spark_task_type,
                                                           spark_task_info=spark_task_info,
                                                           cluster_id=self.streaming_cluster_id, libraries=libraries)
        else:
            return self.client.create_and_run_one_time_job(job_name=job_name, spark_task_type=spark_task_type,
                                                           spark_task_info=spark_task_info,
                                                           cluster_id=self.cluster_id, libraries=libraries)


class DatabricksJobsClient(DatabricksAPIWrapper):
    
    def _create_job_request(self, job_type: str, job_name: str, spark_task_type: str, spark_task_info: dict,
                            cluster_exists: bool = True, cluster_id: Optional[str] = None,
                            libraries: Optional[list] = None, spark_version: str = "7.3.x-scala2.12",
                            node_type_id: str = "Standard_DS3_v2", num_workers: int = 1,
                            auto_scaling: str = True, max_workers: int = 3) -> dict:
        if cluster_exists:
            req_body = {
                job_type: job_name,
                "existing_cluster_id": cluster_id,
                spark_task_type: spark_task_info
            }
        elif auto_scaling:
            req_body = {
                job_type: job_name,
                "new_cluster": {
                    "spark_version": spark_version,
                    "node_type_id": node_type_id,
                    "autoscale": {
                        "min_workers": num_workers,
                        "max_workers": max_workers
                    }
                },
                spark_task_type: spark_task_info
            }
        else:
            req_body = {
                job_type: job_name,
                "new_cluster": {
                    "spark_version": spark_version,
                    "node_type_id": node_type_id,
                    "num_workers": num_workers
                },
                spark_task_type: spark_task_info
            }
        if libraries is not None:
            req_body["libraries"] = libraries
        
        return req_body
    
    def _create_databricks_job_from_job_details(self, job: dict, run_id: Optional[int] = None,
                                                print_json: bool = False) -> DatabricksJobInfo:
        run = None
        if run_id is None:
            query_params = {
                "active_only": True,
                "limit": 1,
                "job_id": int(job["job_id"])
            }
            runs = self.get("/jobs/runs/list", json_params=query_params, print_json=print_json)
            if 'error_code' in runs:
                raise ApiException(
                    f"Error fetching job: {job['job_id']}, from the workspace, \n Error Response: {runs}")
            run = runs["runs"][0]
        else:
            run = self.get("/jobs/runs/get", json_params={"run_id": run_id}, print_json=print_json)
            if 'error_code' in run:
                raise ApiException(
                    f"Error fetching job information from the workspace, \n Error Response: {run}")
        
        life_cycle_state = run["state"]["life_cycle_state"]
        if life_cycle_state in ["TERMINATED", "TERMINATING", "INTERNAL_ERROR"] and \
                "result_state" in run["state"]:
            state = run["state"]["result_state"]
        else:
            state = life_cycle_state
        return DatabricksJobInfo(job_id=job["job_id"],
                                 run_id=run["run_id"],
                                 state=STATE_MAP[state],
                                 start_time=datetime.utcfromtimestamp(int(run["start_time"]) / 1e3),
                                 job_name=job["settings"]["name"])
    
    def get_jobs_list(self, print_json: bool = False) -> list:
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", print_json)
        return jobs.get('jobs', [])
    
    def get_job_id_by_name(self) -> dict:
        """
        get a dict mapping of job name to job id for the new job ids
        :return:
        """
        jobs = self.get_jobs_list()
        job_ids = {}
        for job in jobs:
            job_ids[job['settings']['name']] = job['job_id']
        return job_ids
    
    # Store Job Info in workspace UI and then start the job [Recommended[
    def create_and_run_one_time_job(self, job_name: str, spark_task_type: str, spark_task_info: dict,
                                    cluster_exists: bool = True, cluster_id: Optional[str] = None,
                                    libraries: Optional[list] = None, spark_version: str = "7.3.x-scala2.12",
                                    node_type_id: str = "Standard_DS3_v2", num_workers: int = 1,
                                    auto_scaling: str = True, max_workers: int = 3) -> DatabricksJobInfo:
        if cluster_exists and cluster_id is None:
            raise Exception("Databricks Cluster Id was not provided for the job")
        job_req = self._create_job_request("name", job_name, spark_task_type,
                                           spark_task_info, cluster_exists, cluster_id, libraries, spark_version,
                                           node_type_id, num_workers, auto_scaling, max_workers)
        
        job_response = self.post('/jobs/create', job_req)
        if 'error_code' in job_response:
            raise ApiException(f"Error Launching job : {job_name}, on cluster: "
                               f"{cluster_id if cluster_exists else node_type_id}. \n Error Response: {job_response},"
                               f"\n Error: Request: {job_req}")
        else:
            print(f"Created job configuration, starting job: {job_response}")
            run_response = self.post('/jobs/run-now', job_response)
            if 'error_code' in run_response:
                print(f"Error running job: {job_name} , on cluster: "
                      f"{cluster_id if cluster_exists else node_type_id}.\n Error Response: {run_response}")
            return self.get_databricks_job(job_id=job_response["job_id"], run_id=run_response["run_id"])
    
    # DEPRECATE: Submit job without recording information in Workspace UI
    def run_submit_job(self, job_name: str, spark_task_type: str, spark_task_info: dict,
                       cluster_exists: bool = True, cluster_id: Optional[str] = None,
                       libraries: Optional[list] = None, spark_version: str = "7.3.x-scala2.12",
                       node_type_id: str = "Standard_DS3_v2", num_workers: int = 1, auto_scaling: str = True,
                       max_workers: int = 3) -> DatabricksJobInfo:
        if cluster_exists and cluster_id is None:
            raise Exception("Databricks Cluster Id was not provided for the job")
        
        job_req = self._create_job_request("run_name", job_name, spark_task_type, spark_task_info, cluster_exists,
                                           cluster_id, libraries, spark_version, node_type_id, num_workers,
                                           auto_scaling, max_workers)
        job_response = self.post('/jobs/runs/submit', job_req)
        if 'error_code' in job_response:
            raise ApiException(f"Error Launching job : {job_name}, on cluster: "
                               f"{cluster_id if cluster_exists else node_type_id}. \n Error Response: {job_response}")
        else:
            print(f"Created job configuration, starting job: {job_response}")
            return self.get_databricks_job(int(job_response["run_id"]))
    
    # Get job start time
    def get_job_start_time(self, job_id: int, print_json: bool = False) -> datetime:
        return self.get_databricks_job(job_id, print_json=print_json).start_time
    
    def get_databricks_job(self, job_id: int, run_id: Optional[int] = None,
                           print_json: bool = False) -> DatabricksJobInfo:
        job_response = self.get("/jobs/get", json_params={"job_id": job_id}, print_json=print_json)
        if 'error_code' in job_response:
            raise ApiException(f"Error fetching job information from the workspace, \n Error Response: {job_response}")
        return self._create_databricks_job_from_job_details(job_response, run_id)
    
    def get_active_jobs(self, max_active_jobs: int, cluster_id: str, included_inactive_jobs: bool = False,
                        print_json: bool = False) -> List[DatabricksJobInfo]:
        query_params = {
            "active_only": (not included_inactive_jobs),
            "limit": 1000,
            "run_type": {
                "cluster_spec": {
                    "existing_cluster_id": cluster_id
                }
            }
        }
        job_response = self.get("/jobs/runs/list", json_params=query_params, print_json=print_json)
        if 'error_code' in job_response:
            raise ApiException(f"Error fetching active jobs from the workspace, \n Error Response: {job_response}")
        active_runs = job_response["runs"]
        already_fetched = len(active_runs)
        while bool(job_response["has_more"]) and already_fetched < max_active_jobs:
            query_params["offset"] = already_fetched
            job_response = self.get("/jobs/runs/list", json_params=query_params, print_json=print_json)
            if 'error_code' in job_response:
                raise ApiException(f"Error fetching active jobs from the workspace, \n Error Response: {job_response}")
            active_runs.extend(job_response["runs"])
            already_fetched += len(job_response["runs"])
        active_runs = list(set(active_runs))
        return [self.get_databricks_job(run["job_id"], run["run_id"]) for run in active_runs]
    
    def cancel_job_run(self, job_id: int):
        run_id = self.get_databricks_job(job_id)
        cancel_job_response = self.post('/jobs/runs/cancel', {"run_id": run_id})
        if 'error_code' in cancel_job_response:
            raise ApiException(f"Error Cancelling job: {job_id}, run_id: {run_id}., "
                               f"Error Response: {cancel_job_response}")


def _submit_job(api: DatabricksJobManager, job_type: str, job_extra_info: dict, spark_task_type: str,
                spark_task_info: dict, libraries: list = None, use_stream_cluster: bool = False) -> DatabricksJobInfo:
    return api.create_spark_job(job_type=job_type, job_extra_info=job_extra_info, spark_task_type=spark_task_type,
                                spark_task_info=spark_task_info, libraries=libraries,
                                use_stream_cluster=use_stream_cluster)


def _list_jobs(api: DatabricksJobManager, include_terminated: bool = False, project: Optional[str] = None,
               table_name: Optional[str] = None) -> List[DatabricksJobInfo]:
    return api.get_all_spark_jobs(include_terminated, project, table_name)


def _get_job_by_id(api: DatabricksJobManager, job_id: int) -> Optional[DatabricksJobInfo]:
    try:
        return api.get_spark_job(job_id)
    except client.ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def _cancel_job_by_id(api: DatabricksJobManager, job_id: int):
    try:
        api.cancel_spark_job(job_id)
    except client.ApiException as e:
        if e.status == 404:
            return None
        else:
            raise
