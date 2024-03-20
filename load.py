import duckdb
import pandas as pd
from logzero import logging


def load_data(records: list[dict]):
	r = records[0]
	dict_df = {
		"prices": pd.DataFrame(r["prices"], columns=["epoch", "price"]),
		"market_caps": pd.DataFrame(r["market_caps"], columns=["epoch", "market_cap"]),
		"total_volumes": pd.DataFrame(r["total_volumes"], columns=["epoch", "total_volume"])
		}

	# create the tables from the DataFrame in dict
	# Note: duckdb.sql connects to the default in-memory database connection
	for table_name, df in dict_df.items():
		logging.info(f"Create table: {table_name} from Dataframe: {df}")
		duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROMdf ")
		logging.info(f"Table {table_name} is populated")


if __name__ == "__main__":
	from extract import get_records

	records = get_records("1", "08-03-2024")
	load_data(records)


