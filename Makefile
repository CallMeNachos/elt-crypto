## Development
VENV := .venv
PYTHON := $(VENV)/bin/python

$(PYTHON):
	python3 -m pip install virtualenv
	python3 -m virtualenv -p /usr/bin/python3 $(VENV)
install:
	$(PYTHON) -m pip install poetry
	poetry install
py-ingest:
	poetry run python3 -m elt.load
py-viz:
	poetry run python3 -m dataviz.candlestick