# Convenience targets. Anything here can be run by hand - see README.
PY ?= python3
export PYTHONPATH := src
ROWS ?= 100000

.PHONY: help install sample test run run-baseline benchmark reconcile clean

help:
	@echo "make install    install python dependencies"
	@echo "make sample     generate the seeded sample dataset (about 2k orders)"
	@echo "make test       run the test suite on a local spark session"
	@echo "make run        run the optimized pipeline end to end"
	@echo "make benchmark  baseline vs optimized on ROWS items (default 100000)"
	@echo "make reconcile  print the reconciliation report"
	@echo "make clean      remove generated outputs (keeps data/raw)"

install:
	$(PY) -m pip install -r requirements.txt

sample:
	$(PY) -m pipeline generate --rows 2000 --seed 42

test:
	$(PY) -m pytest -q

run:
	$(PY) -m pipeline run --strategy optimized

run-baseline:
	$(PY) -m pipeline run --strategy baseline

benchmark:
	$(PY) -m pipeline benchmark --rows $(ROWS)

reconcile:
	$(PY) -m pipeline reconcile

clean:
	rm -rf data/curated data/quarantine data/runs spark-warehouse derby.log metastore_db
