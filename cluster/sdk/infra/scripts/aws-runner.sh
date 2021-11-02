#!/usr/bin/env bash

set -euo pipefail

GIT_TAG=${PULL_PULL_SHA:-${PULL_BASE_SHA}}

source infra/scripts/k8s-common-functions.sh
wait_for_image "${DOCKER_REPOSITORY}" feast-jobservice "${GIT_TAG}"

infra/scripts/codebuild_runner.py "$@"