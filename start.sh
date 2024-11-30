#!/bin/bash

export SERVICE_ADDRESSES="http://localhost:8081;http://localhost:8082;http://localhost:8083"

uvicorn rest_api:app --port 8081 --log-level error &
uvicorn rest_api:app --port 8082 --log-level error &
uvicorn rest_api:app --port 8083 --log-level error
