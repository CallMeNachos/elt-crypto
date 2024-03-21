import airbyte as ab


def get_records(days: int, start_date: str) -> list[dict]:
	# Create and configure the source connector:
	source = ab.get_source(
		'source-coingecko-coins',
		config={
			"environment": "sandbox",
			"coin_id": "bitcoin",
			"vs_currency": "usd",
			"days": str(days),  # 1,
			"start_date": start_date,  # "08-03-2024"
			# "end_date": "10-03-2024"
			}
		)

	# Verify the config and creds by running `check`:
	try:
		source.check()
	except ConnectionError as exc:
		raise RuntimeError('Failed to connect to the API') from exc

	# history_records = list(source.get_records(stream="history"))
	records = list(source.get_records(stream="market_chart"))

	return records


if __name__ == "__main__":
	pass
	# result = ab.get_available_connectors()
	# print([i for i in result if i ])

	# source.select_all_streams()
	# Read data from the source into the default cache:

	# cache = ab.get_default_cache()
	# result = source.read(cache=cache)

