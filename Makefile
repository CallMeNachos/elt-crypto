## Development
install:
	poetry install
py-ingest:
	poetry run python3 -m elt.load
py-viz:
	poetry run python3 -m dataviz.candlestick