#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
cd $SCRIPT_DIR
docker rm spectrum-discover-inference-api
docker rmi ibmcom/spectrum-discover-inference-api
docker build -t ibmcom/spectrum-discover-inference-api .
