UV ?= uv
ENV_FILE ?= .env

.PHONY: install upload download

install:
	@test -f "$(ENV_FILE)" || cp .env.example "$(ENV_FILE)"
	$(UV) sync --locked

upload:
	$(UV) run vps-sync --env-file "$(ENV_FILE)" upload

download:
	$(UV) run vps-sync --env-file "$(ENV_FILE)" download
