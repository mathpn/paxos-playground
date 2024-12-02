#!/bin/bash

export SERVICE_ADDRESSES="http://localhost:8081;http://localhost:8082;http://localhost:8083"
export N_INSTANCES=3

INSTANCE_ID=0 uvicorn rest_api:app --port 8081 --log-level error &
INSTANCE_ID=1 uvicorn rest_api:app --port 8082 --log-level error &
INSTANCE_ID=2 uvicorn rest_api:app --port 8083 --log-level error
# sleep 1 && curl -X 'POST' \
# 	'http://localhost:8081/propose?value=foo' \
# 	-H 'accept: application/json' \
# 	-d ''
#
# pkill -P $$
