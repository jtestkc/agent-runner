.PHONY: help dev test build-sandbox lint fmt scan run-worker run-api compose-up compose-down

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

help:
	@echo "make dev test build-sandbox lint fmt scan run-api run-worker compose-up compose-down"

$(VENV):
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip

.venv-stamp: $(VENV) pyproject.toml
	$(PIP) install -e ".[dev]"
	touch .venv-stamp

dev:
	docker compose up --build

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

test: .venv-stamp
	PYTHONPATH=src $(PY) -m pytest tests/ -q

lint: .venv-stamp
	$(PY) -m ruff check src tests
	$(PY) -m black --check src tests

fmt: .venv-stamp
	$(PY) -m black src tests
	$(PY) -m ruff check --fix src tests

build-sandbox:
	./scripts/build_rootfs.sh

scan: .venv-stamp
	$(PY) -m pip-audit -e .
	-docker run --rm -v $$(pwd):/app aquasec/trivy:latest image agent-runner:latest

run-api: .venv-stamp
	PYTHONPATH=src AGENT_RUNNER_sandbox_backend=docker $(PY) -m agent_runner.api

run-worker: .venv-stamp
	PYTHONPATH=src AGENT_RUNNER_sandbox_backend=docker $(PY) -m agent_runner.worker
