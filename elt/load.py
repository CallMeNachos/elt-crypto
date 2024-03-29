import duckdb
import pandas as pd
from logzero import logger
import os


def load_data(records: list[dict]):
	logger.info(f"Loading data" + "." * 10)

	r = records[0]
	dict_df = {
		"raw_prices": pd.DataFrame(r["prices"], columns=["epoch", "price"]),
		"raw_market_caps": pd.DataFrame(r["market_caps"], columns=["epoch", "market_cap"]),
		"raw_total_volumes": pd.DataFrame(r["total_volumes"], columns=["epoch", "total_volume"])
		}

	# Create the tables from the DataFrame in dict
	# Note: duckdb.sql connects to the default in-memory database connection
	for table_name, df in dict_df.items():
		show_tables = duckdb.sql("SHOW ALL TABLES").df()
		if table_name in list(show_tables["name"]):
			logger.info(f"Insert table: {table_name} ...")
			duckdb.sql(f"INSERT {table_name} AS SELECT * FROM df")
			logger.info(f"Table {table_name} is populated")
		else:
			logger.info(f"Create table: {table_name} ...")
			duckdb.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
			logger.info(f"Table {table_name} is populated")

	# Create csv files from the dataframes
	abspath = os.path.dirname(os.path.abspath(__file__))
	for table_name, df in dict_df.items():
		df.to_csv(abspath + "/transform/dbt_crypto/seeds/" + table_name + ".csv", index=False, encoding="utf-8")
		df.to_csv(os.path.dirname(abspath) + "/dataviz/tables/" + table_name + ".csv", index=False, encoding="utf-8")


if __name__ == "__main__":
	from elt.extract import get_records
	rows = get_records("bitcoin", 7, "10-03-2024")
	load_data(rows)
