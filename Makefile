.PHONY: help install dev-install test lint format clean build deploy

help:
	@echo "Available commands:"
	@echo "  install       Install production dependencies"
	@echo "  dev-install   Install development dependencies"
	@echo "  test          Run tests"
	@echo "  lint          Run linting"
	@echo "  format        Format code"
	@echo "  clean         Clean build artifacts"
	@echo "  build         Build package"
	@echo "  deploy        Deploy to Databricks"

install:
	pip install -r requirements.txt

dev-install:
	pip install -r requirements/development.txt
	pre-commit install

test:
	pytest tests/ -v --cov=aurora_cdc --cov-report=html

lint:
	flake8 src/ tests/
	mypy src/

format:
	black src/ tests/
	isort src/ tests/

clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache/ .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

deploy:
	./scripts/deployment/deploy_to_databricks.sh

# Docker commands
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

# Infrastructure commands
infra-deploy:
	cd infrastructure/scripts && ./deploy-infrastructure.sh $(ENV)

infra-destroy:
	cd infrastructure/scripts && ./cleanup.sh $(ENV)

# Database commands
db-setup:
	./infrastructure/scripts/setup-database.sh $(DB_HOST) $(DB_USER) $(DB_PASSWORD)

db-seed:
	python scripts/data/generate_sample_data.py

# Monitoring commands
metrics-start:
	python src/aurora_cdc/monitoring/metrics.py

health-check:
	python scripts/monitoring/health_check.py