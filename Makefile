PY ?= python3

.PHONY: setup data pipeline eval-dq forecast site all sample test

setup:            ## install dependencies into the current environment
	$(PY) -m pip install -r requirements.txt

data:             ## download the French archives and convert 12 months to the canonical raw format
	mkdir -p data/fr
	curl -fL -o data/fr/2025.zip https://donnees.roulez-eco.fr/opendata/annee/2025
	curl -fL -o data/fr/2026.zip https://donnees.roulez-eco.fr/opendata/annee/2026
	$(PY) -m fuelcycle.sources.fr data/fr/2025.zip data/fr/2026.zip --out data/raw --start 2025-11-01 --end 2026-10-31

pipeline:         ## raw -> bronze -> silver -> gold, month by month
	$(PY) -m fuelcycle.pipeline --full-refresh

eval-dq:          ## fault-injection score of the data-quality gate
	$(PY) -m fuelcycle.eval_dq --days 14

forecast:         ## rolling-origin backtest of the next-day forecast
	$(PY) -m fuelcycle.forecast --test-weeks 8

site:             ## export JSON for the demo page
	$(PY) -m fuelcycle.export_site

all: pipeline eval-dq forecast site

test:             ## end-to-end tests on a synthetic sample
	$(PY) -m pytest -q tests
