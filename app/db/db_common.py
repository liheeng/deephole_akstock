from utils.common import is_running_in_docker

data_volume = "/data" if is_running_in_docker() else "./data"
DB = data_volume + "/stock.duckdb"
