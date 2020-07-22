#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
cd $SCRIPT_DIR
docker run -d --name dbagent  --mount 'type=bind,src=/gpfs/gpfs0/connections/scale/id_rsa,dst=/keys/id_rsa' --env-file "$SCRIPT_DIR/vars.txt" ibmcom/db-metadata-agent
sleep 5
docker logs dbagent -f