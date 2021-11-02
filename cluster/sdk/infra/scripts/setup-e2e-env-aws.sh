#!/bin/bash

python -m pip install --upgrade pip==20.2 setuptools wheel

make install-python

python -m pip install -qr tests/requirements.txt

# Using mvn -q to make it less verbose. This step happens after docker containers were
# succesfully built so it should be unlikely to fail, therefore we likely won't need detailed logs.
echo "########## Building ingestion jar"
TIMEFORMAT='########## took %R seconds'

time make build-ingestion-jar-no-tests REVISION=develop MAVEN_EXTRA_OPTS="-q --no-transfer-progress"
