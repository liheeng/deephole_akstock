#!/bin/bash

set -o allexport
source .env.dev
set +o allexport

docker compose --env-file .env.dev down
docker rmi akshare-stock:"${IMAGE_VERSION}"