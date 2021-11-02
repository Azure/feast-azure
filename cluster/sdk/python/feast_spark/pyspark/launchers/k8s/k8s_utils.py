import hashlib
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from kubernetes import client, config
from kubernetes.client.api import CustomObjectsApi

from feast_spark.pyspark.abc import SparkJobStatus

__all__ = [
    "_get_api",
    "_cancel_job_by_id",
    "_prepare_job_resource",
    "_prepare_scheduled_job_resource",
    "_list_jobs",
    "_get_job_by_id",
    "_generate_project_table_hash",
    "STREAM_TO_ONLINE_JOB_TYPE",
    "OFFLINE_TO_ONLINE_JOB_TYPE",
    "HISTORICAL_RETRIEVAL_JOB_TYPE",
    "METADATA_JOBHASH",
    "METADATA_OUTPUT_URI",
    "JobInfo",
]

STREAM_TO_ONLINE_JOB_TYPE = "STREAM_TO_ONLINE_JOB"
OFFLINE_TO_ONLINE_JOB_TYPE = "OFFLINE_TO_ONLINE_JOB"
HISTORICAL_RETRIEVAL_JOB_TYPE = "HISTORICAL_RETRIEVAL_JOB"

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


def _append_items(resource: Dict[str, Any], path: Tuple[str, ...], items: List[Any]):
    """ A helper function to manipulate k8s resource configs. It updates an array in resource
        definition given a jsonpath-like path. Will not update resource if items is empty.
        Note that it updates resource dict in-place.

    Examples:
        >>> _append_items({}, ("foo", "bar"), ["A", "B"])
        {'foo': {'bar': ['A', 'B']}}

        >>> _append_items({"foo": {"bar" : ["C"]}}, ("foo", "bar"), ["A", "B"])
        {'foo': {'bar': ['C', 'A', 'B']}}

        >>> _append_items({}, ("foo", "bar"), [])
        {}
    """

    if not items:
        return resource

    obj = resource
    for i, p in enumerate(path):
        if p not in obj:
            if i == len(path) - 1:
                obj[p] = []
            else:
                obj[p] = {}
        obj = obj[p]
    assert isinstance(obj, list)
    obj.extend(items)
    return resource


def _add_keys(resource: Dict[str, Any], path: Tuple[str, ...], items: Dict[str, Any]):
    """ A helper function to manipulate k8s resource configs. It will update a dict in resource
        definition given a path (think jsonpath). Will ignore items set to None. Will not update
        resource if all items are None. Note that it updates resource dict in-place.

    Examples:
        >>> _add_keys({}, ("foo", "bar"), {"A": 1, "B": 2})
        {'foo': {'bar': {'A': 1, 'B': 2}}}

        >>> _add_keys({}, ("foo", "bar"), {"A": 1, "B": None})
        {'foo': {'bar': {'A': 1}}}

        >>> _add_keys({}, ("foo", "bar"), {"A": None, "B": None})
        {}
    """

    if not any(i is not None for i in items.values()):
        return resource

    obj = resource
    for p in path:
        if p not in obj:
            obj[p] = {}
        obj = obj[p]

    for k, v in items.items():
        if v is not None:
            obj[k] = v
    return resource


def _generate_project_table_hash(project: str, table_name: str) -> str:
    return hashlib.md5(f"{project}:{table_name}".encode()).hexdigest()


def _job_id_to_resource_name(job_id: str) -> str:
    return job_id


def _prepare_job_resource(
    job_template: Dict[str, Any],
    job_id: str,
    job_type: str,
    main_application_file: str,
    main_class: Optional[str],
    packages: List[str],
    jars: List[str],
    extra_metadata: Dict[str, str],
    azure_credentials: Dict[str, str],
    arguments: List[str],
    namespace: str,
    extra_labels: Dict[str, str] = None,
) -> Dict[str, Any]:
    """ Prepare SparkApplication custom resource configs """
    job = deepcopy(job_template)

    labels = {LABEL_JOBID: job_id, LABEL_JOBTYPE: job_type}
    if extra_labels:
        labels = {**labels, **extra_labels}

    _add_keys(job, ("metadata", "labels"), labels)
    _add_keys(
        job,
        ("metadata",),
        dict(name=_job_id_to_resource_name(job_id), namespace=namespace),
    )
    _add_keys(job, ("spec",), dict(mainClass=main_class))
    _add_keys(job, ("spec",), dict(mainApplicationFile=main_application_file))
    _add_keys(job, ("spec",), dict(arguments=arguments))

    _add_keys(job, ("spec", "sparkConf"), extra_metadata)
    _add_keys(job, ("spec", "sparkConf"), azure_credentials)

    if len(packages) > 1:
        _append_items(job, ("spec", "deps", "packages"), packages)
    _append_items(job, ("spec", "deps", "jars"), jars)

    return job


def _prepare_scheduled_job_resource(
    scheduled_job_template: Dict[str, Any],
    scheduled_job_id: str,
    job_schedule: str,
    job_template: Dict[str, Any],
    job_type: str,
    main_application_file: str,
    main_class: Optional[str],
    packages: List[str],
    jars: List[str],
    extra_metadata: Dict[str, str],
    azure_credentials: Dict[str, str],
    arguments: List[str],
    namespace: str,
    extra_labels: Dict[str, str] = None,
) -> Dict[str, Any]:
    """ Prepare ScheduledSparkApplication custom resource configs """
    scheduled_job = deepcopy(scheduled_job_template)
    _add_keys(scheduled_job, ("spec",), dict(schedule=job_schedule))

    labels = {LABEL_JOBID: scheduled_job_id, LABEL_JOBTYPE: job_type}
    if extra_labels:
        labels = {**labels, **extra_labels}

    _add_keys(scheduled_job, ("metadata", "labels"), labels)
    _add_keys(
        scheduled_job, ("metadata",), dict(name=scheduled_job_id, namespace=namespace)
    )

    job = _prepare_job_resource(
        job_template=job_template,
        job_id=scheduled_job_id,
        job_type=job_type,
        main_application_file=main_application_file,
        main_class=main_class,
        packages=packages,
        jars=jars,
        extra_metadata=extra_metadata,
        azure_credentials=azure_credentials,
        arguments=arguments,
        namespace=namespace,
        extra_labels=extra_labels,
    )

    _add_keys(scheduled_job, ("spec", "template"), job["spec"])
    return scheduled_job


def _get_api(incluster: bool) -> CustomObjectsApi:
    # Configs can be set in Configuration class directly or using helper utility
    if not incluster:
        config.load_kube_config()
    else:
        config.load_incluster_config()

    return client.CustomObjectsApi()


def _crd_args(namespace: str) -> Dict[str, str]:
    return dict(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=namespace,
        plural="sparkapplications",
    )


def _scheduled_crd_args(namespace: str) -> Dict[str, str]:
    return dict(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=namespace,
        plural="scheduledsparkapplications",
    )


class JobInfo(NamedTuple):
    job_id: str
    job_type: str
    job_error_message: str
    namespace: str
    extra_metadata: Dict[str, str]
    state: SparkJobStatus
    labels: Dict[str, str]
    start_time: datetime


STATE_MAP = {
    "": SparkJobStatus.STARTING,
    "SUBMITTED": SparkJobStatus.STARTING,
    "RUNNING": SparkJobStatus.IN_PROGRESS,
    "COMPLETED": SparkJobStatus.COMPLETED,
    "FAILED": SparkJobStatus.FAILED,
    "SUBMISSION_FAILED": SparkJobStatus.FAILED,
    "PENDING_RERUN": SparkJobStatus.STARTING,
    "INVALIDATING": SparkJobStatus.STARTING,
    "SUCCEEDING": SparkJobStatus.IN_PROGRESS,
    "FAILING": SparkJobStatus.FAILED,
}


def _k8s_state_to_feast(k8s_state: str) -> SparkJobStatus:
    return STATE_MAP[k8s_state]


def _resource_to_job_info(resource: Dict[str, Any]) -> JobInfo:
    labels = resource["metadata"]["labels"]
    start_time = datetime.strptime(
        resource["metadata"].get("creationTimestamp"), "%Y-%m-%dT%H:%M:%SZ"
    )
    sparkConf = resource["spec"].get("sparkConf", {})

    if "status" in resource:
        state = _k8s_state_to_feast(resource["status"]["applicationState"]["state"])
        error_message = (
            resource["status"].get("applicationState", {}).get("errorMessage", "")
        )
    else:
        state = _k8s_state_to_feast("")
        error_message = ""

    return JobInfo(
        job_id=labels[LABEL_JOBID],
        job_type=labels.get(LABEL_JOBTYPE, ""),
        job_error_message=error_message,
        namespace=resource["metadata"].get("namespace", "default"),
        extra_metadata={k: v for k, v in sparkConf.items() if k in METADATA_KEYS},
        state=state,
        labels=labels,
        start_time=start_time,
    )


def _submit_job(api: CustomObjectsApi, resource, namespace: str) -> JobInfo:
    # create the resource
    response = api.create_namespaced_custom_object(
        **_crd_args(namespace), body=resource,
    )
    return _resource_to_job_info(response)


def _submit_scheduled_job(
    api: CustomObjectsApi, name: str, resource: dict, namespace: str
):
    try:
        api.get_namespaced_custom_object(**_scheduled_crd_args(namespace), name=name)
    except client.ApiException as e:
        if e.status == 404:
            api.create_namespaced_custom_object(
                **_scheduled_crd_args(namespace), body=resource
            )
            return
        else:
            raise e
    api.patch_namespaced_custom_object(
        **_scheduled_crd_args(namespace), name=name, body=resource,
    )


def _list_jobs(
    api: CustomObjectsApi,
    namespace: str,
    project: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[JobInfo]:
    result = []

    # Batch, Streaming Ingestion jobs
    if project and table_name:
        table_name_hash = _generate_project_table_hash(project, table_name)
        response = api.list_namespaced_custom_object(
            **_crd_args(namespace),
            label_selector=f"{LABEL_FEATURE_TABLE_HASH}={table_name_hash}",
        )
    elif project:
        response = api.list_namespaced_custom_object(
            **_crd_args(namespace), label_selector=f"{LABEL_PROJECT}={project}",
        )
    else:
        # Retrieval jobs
        response = api.list_namespaced_custom_object(
            **_crd_args(namespace), label_selector=LABEL_JOBID,
        )

    for item in response["items"]:
        result.append(_resource_to_job_info(item))
    return result


def _get_job_by_id(
    api: CustomObjectsApi, namespace: str, job_id: str
) -> Optional[JobInfo]:
    try:
        response = api.get_namespaced_custom_object(
            **_crd_args(namespace), name=_job_id_to_resource_name(job_id)
        )

        return _resource_to_job_info(response)
    except client.ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def _cancel_job_by_id(api: CustomObjectsApi, namespace: str, job_id: str):
    try:
        api.delete_namespaced_custom_object(
            **_crd_args(namespace), name=_job_id_to_resource_name(job_id),
        )
    except client.ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def _unschedule_job(api: CustomObjectsApi, namespace: str, resource_name: str):
    try:
        api.delete_namespaced_custom_object(
            **_scheduled_crd_args(namespace), name=resource_name,
        )
    except client.ApiException as e:
        if e.status != 404:
            raise


DEFAULT_JOB_TEMPLATE = """

apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/kf-feast/spark-py:v3.0.1"
  imagePullPolicy: Always
  sparkVersion: "3.0.1"
  timeToLiveSeconds: 3600
  pythonVersion: "3"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 3.0.1
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 3.0.1
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
"""

DEFAULT_SCHEDULED_JOB_TEMPLATE = """

apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: ScheduledSparkApplication
metadata:
  namespace: default
spec:
  schedule: "0 1 * * *"
"""
