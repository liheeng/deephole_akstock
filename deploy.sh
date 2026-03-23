#!/bin/bash

set -o allexport
source .env.dev
set +o allexport

docker build -t akshare-stock:"${IMAGE_VERSION}" .
docker compose --env-file .env.dev up akstock_db_init
docker compose --env-file .env.dev up -d akstock_stock_fetcher
docker compose --env-file .env.dev up -d akstock_api_service
docker compose --env-file .env.dev up -d akstock_dashboard