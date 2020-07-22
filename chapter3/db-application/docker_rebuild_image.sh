#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

docker stop dbagent
docker rm dbagent
docker rmi ibmcom/db-metadata-agent
cd $SCRIPT_DIR
docker build -t ibmcom/db-metadata-agent .

