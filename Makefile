# =============================================================================
# Makefile — the single entry point for the whole pipeline
# =============================================================================
# The README promises that `make build` rebuilds the warehouse end-to-end from
# raw bags. That promise is only worth anything if it is the *only* command
# anyone needs to remember, so every step below is a prerequisite of `build`
# rather than a separate thing to run in the right order by hand.
#
# Usage:  make build     # generate bags -> land bronze -> dbt silver/gold -> test
#         make verify    # rebuild twice and prove the gold layer is identical
# =============================================================================

VENV   := .venv
PY     := $(VENV)/bin/python
DBT    := $(VENV)/bin/dbt

# dbt finds profiles.yml here instead of ~/.dbt, so the project needs no
# machine-level setup and stays clone-and-run.
export DBT_PROFILES_DIR := .
# The duckdb file the warehouse lives in. `verify` overrides this to compare
# two independent rebuilds without clobbering the working copy.
export ROS_DUCKDB_PATH ?= dev.duckdb

.PHONY: help setup deps bags ingest build silver gold test docs clean verify

help:
	@echo "build   generate bags -> land bronze -> dbt silver/gold -> test"
	@echo "verify  rebuild twice and prove the gold layer is byte-identical"
	@echo "test    run dbt data-quality tests"
	@echo "docs    generate the dbt docs site"

## setup: first-time setup — venv + dependencies (~30s)
setup:
	uv venv $(VENV)
	uv pip install --python $(PY) -e ".[dev]"

## deps: install/refresh dependencies into the existing venv
deps:
	uv pip install --python $(PY) -e ".[dev]"

## bags: generate the synthetic ROS 2 bags from a fixed seed
bags:
	$(PY) -m generate.make_bags

## ingest: read the raw bags and land them as partitioned bronze Parquet
ingest:
	$(PY) -m ingest.land_bronze

## build: the full pipeline, in dependency order
build: bags ingest
	$(DBT) build

## silver / gold / test: individual dbt steps when iterating
silver:
	$(DBT) run --select silver

gold:
	$(DBT) run --select gold

test:
	$(DBT) test

docs:
	$(DBT) docs generate

## clean: remove generated bags, the warehouse, and dbt artifacts
clean:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('data/raw','data/bronze','data/silver','data/gold','target','dbt_packages','logs')]"
	$(PY) -c "import pathlib; [p.unlink() for p in pathlib.Path('.').glob('*.duckdb*')]"

## verify: the idempotency guarantee, checked rather than asserted.
## Rebuilds the warehouse from scratch into two separate duckdb files and
## compares the exported gold tables. If a re-run of the same bags ever produced
## different gold data, this fails — which is the whole point.
verify:
	$(MAKE) clean
	ROS_DUCKDB_PATH=verify_a.duckdb $(MAKE) build
	$(PY) -m tools.verify_idempotency export --db verify_a.duckdb --out .verify/a
	$(MAKE) clean
	ROS_DUCKDB_PATH=verify_b.duckdb $(MAKE) build
	$(PY) -m tools.verify_idempotency export --db verify_b.duckdb --out .verify/b
	$(PY) -m tools.verify_idempotency diff --a .verify/a --b .verify/b
