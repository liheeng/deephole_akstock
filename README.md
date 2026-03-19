# README
## Commands

### Stop and remove all akstock containers

``` bash
> docker compose --env-file .env.dev down
```

### Delete asstock docker image

``` bash
> docker rmi asshare-stock:1.0
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