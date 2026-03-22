docker build -t akshare-stock:1.1 .
docker compose --env-file .env.dev up akstock_db_init
docker compose --env-file .env.dev up -d akstock_stock_fetcher
docker compose --env-file .env.dev up -d akstock_api_service
docker compose --env-file .env.dev up -d akstock_dashboard