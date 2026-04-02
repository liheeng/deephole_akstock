#!/bin/bash
ENV_FILE=${1:-.env.dev}
set -o allexport
source "$ENV_FILE"
set +o allexport

docker compose -f docker-compose-final.yaml --env-file "$ENV_FILE" down
docker rmi -f "${_TEMP_IMAGE_NAME_}:${_TEMP_IMAGE_VERSION}"