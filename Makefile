.DEFAULT_GOAL := help

UV ?= uv
ENV_FILE ?= .env

.PHONY: help setup sync upload download check clean

help:
	@echo "make setup     - Initialize config and dependencies"
	@echo "make upload    - Upload changed files"
	@echo "make download  - Download changed files"
	@echo "make check     - Run code quality checks"
	@echo "make clean     - Remove generated artifacts"

setup:
	@test -f "$(ENV_FILE)" || cp .env.example "$(ENV_FILE)"
	$(UV) sync --locked --dev

sync:
	$(UV) sync --locked --dev

upload:
	$(UV) run vps-sync --env-file "$(ENV_FILE)" upload

download:
	$(UV) run vps-sync --env-file "$(ENV_FILE)" download

check:
	$(UV) run ruff check src
	$(UV) run mypy

clean:
	rm -rf build dist .pytest_cache .mypy_cache .ruff_cache .coverage
	find src -type d -name '__pycache__' -prune -exec rm -rf {} +
