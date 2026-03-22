# Summary

* This is a dockerized stock daily data fetcher and api service, currently support CN, HK and US market.
* A web dashboard is provided, you can start sync task to download CN, HK and US market daily data, check task status and logs, the defaut URL is http://localhost:8051.
* It has a cron job to sync daily data in daemon, the default schedule is 18:00 every day.(Disabled in version 1.1, will release in version 1.2)
* It also provides a set of API, the API is based on FastAPI and URL is http://localhost:8000, you can use it to get stock daily data.

# Quick start

## Deploy and undeploy

### Deploy

* Make sure you network can access official docker registry (https://hub.docker.com/).
* Run deploy.sh, it will build docker image and run containers.

### Undeploy

* Run undeploy.sh, it will stop all containers and remove docker image.

## How to use

* After deploy, you can access dashboard, open URL http://localhost:8051 in your browser, like chrome and so on.
* In dashboard page, you can start sync task to download CN, HK and US daily data, check task status and logs.

## Data and logs

* The duckdb is used, now only has single database name stock.duckdb in project data folder.
* All market daily data save only stock_daily table.

**Note:** DO NOT open or access stock.duckdb in any way while the current application is running.Concurrent access may corrupt or permanently damage your database file!
  
## Commands

### Stop and remove all akstock containers

``` bash
> docker compose --env-file .env.dev down
```

### Delete asstock docker image

``` bash
> docker rmi akshare-stock:1.0
```

### Create akstock docker image

``` bash
> docker build -t akshare-stock:1.0 .
```

### Init akstock_db_init container

``` bash
> docker compose --env-file .env.dev up akstock_db_init
```

### Run other aksock containers

``` bash
> docker compose --env-file .env.dev up -d akstock_stock_fetcher
> docker compose --env-file .env.dev up -d akstock_api_service
> docker compose --env-file .env.dev up -d akstock_dashboard
```

### Only stop containers

``` bash
> bash stop_containers.sh
```
