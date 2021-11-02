#!/usr/bin/env bash

export DISABLE_SERVICE_FIXTURES=1
export GIT_TAG=${PULL_PULL_SHA:-${PULL_BASE_SHA}}
export GIT_REMOTE_URL=https://github.com/feast-dev/feast-spark.git

export MAVEN_OPTS="-Dmaven.repo.local=/tmp/.m2/repository -DdependencyLocationsEnabled=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"
export MAVEN_CACHE="gs://feast-templocation-kf-feast/.m2.2020-11-17.tar"

NAMESPACE=default

test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"
test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"
test -z ${KUBE_CLUSTER} && KUBE_CLUSTER="feast-e2e-dataflow"

test -z ${DOCKER_REPOSITORY} && DOCKER_REPOSITORY="gcr.io/kf-feast"

infra/scripts/download-maven-cache.sh --archive-uri ${MAVEN_CACHE} --output-dir /tmp
apt-get update && apt-get install -y redis-server postgresql libpq-dev

source infra/scripts/k8s-common-functions.sh

# Prepare gcloud sdk
gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud -q auth configure-docker

gcloud config set project ${GCLOUD_PROJECT}
gcloud config set compute/region ${GCLOUD_REGION}
gcloud config list

gcloud container clusters get-credentials ${KUBE_CLUSTER} --region ${GCLOUD_REGION} --project ${GCLOUD_PROJECT}

k8s_cleanup "feast-release" "$NAMESPACE"
k8s_cleanup "js" "$NAMESPACE"

wait_for_image "${DOCKER_REPOSITORY}" feast-jobservice "${GIT_TAG}"

# Install components via helm
helm_install "js" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "$NAMESPACE" \
  --set "feast-jobservice.envOverrides.FEAST_CORE_URL=feast-release-feast-core:6565" \
  --set "feast-jobservice.envOverrides.FEAST_SPARK_LAUNCHER=dataproc" \
  --set "feast-jobservice.envOverrides.FEAST_DATAPROC_CLUSTER_NAME=feast-e2e" \
  --set "feast-jobservice.envOverrides.FEAST_DATAPROC_PROJECT=kf-feast" \
  --set "feast-jobservice.envOverrides.FEAST_DATAPROC_REGION=us-central1" \
  --set "feast-jobservice.envOverrides.FEAST_SPARK_STAGING_LOCATION=gs://feast-templocation-kf-feast/" \
  --set "feast-jobservice.envOverrides.FEAST_REDIS_HOST=10.128.0.62" \
  --set "feast-jobservice.envOverrides.FEAST_REDIS_PORT=6379" \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].type=REDIS_CLUSTER' \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].name=online' \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].config.connection_string=10.128.0.62:6379' \
  --set "redis.enabled=false" \
  --set "kafka.enabled=false"

CMD=$(printf '%s' \
  "mkdir src && cd src && git clone --recursive ${GIT_REMOTE_URL} && cd feast-spark && " \
  "git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && " \
  "git fetch -q && git checkout ${GIT_TAG} && " \
  "make install-python && " \
  "python -m pip install -qr tests/requirements.txt && " \
  "pytest -v tests/e2e/ --env gcloud " \
  "--staging-path gs://feast-templocation-kf-feast/ " \
  "--core-url feast-release-feast-core:6565 " \
  "--serving-url feast-release-feast-online-serving:6566 " \
  "--job-service-url js-feast-jobservice:6568 " \
  "--kafka-brokers 10.128.0.103:9094 --bq-project kf-feast --feast-version dev -m \"not k8s\"")

# Delete old test running pod if it exists
kubectl delete pod -n "$NAMESPACE" ci-test-runner 2>/dev/null || true

kubectl run -n "$NAMESPACE" -i ci-test-runner  \
    --pod-running-timeout=5m \
    --restart=Never \
    --image="${DOCKER_REPOSITORY}/feast-ci:latest" \
    --env="FEAST_TELEMETRY=false" \
    --env="DISABLE_FEAST_SERVICE_FIXTURES=1" \
    --env="DISABLE_SERVICE_FIXTURES=1" \
    -- bash -c "$CMD"

