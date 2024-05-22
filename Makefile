.PHONY: clean clean-test clean-pyc clean-build build help
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

pre-commit-setup: ## Install pre-commit
	python3 -m pip install pre-commit
	pre-commit --version

pre-commit: ## Run pre-commit
	pre-commit run --all-files

create-venv:
	python3 -m venv .venv
	source .venv/bin/activate

requirements:
	python3 -m pip install -r requirements.txt
	python3 -m pip install pytest-asyncio==0.23.6
	python3 -m pip install httpx==0.27.0
	python3 -m pip install pre-commit==3.6.2
	python3 -m pip install ruff==0.1.15
	pre-commit install

docker-restart:
	docker compose down
	docker compose up --build -d

docker-test:
	docker compose down
	docker compose up --build -d
	pytest

api-setup:
	python3 -m pip install "fastapi[all]"

env-setup:
	touch .env
	echo "KAFKA_HOST= 'localhost:9092'" >> .env
	echo "DATABASE_URL= 'mysql+aiomysql://root:root_bot_buster@localhost:3307/playerdata'"  >> .env
	echo "ENV='DEV'" >> .env
	echo "POOL_RECYCLE='60'" >> .env
	echo "POOL_TIMEOUT='30'" >> .env

docs:
	open http://localhost:5000/docs
	xdg-open http://localhost:5000/docs
	. http://localhost:5000/docs