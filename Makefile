# Set the current user's UID for Airflow
AIRFLOW_UID := $(shell id -u)

# Download docker-compose.yaml if it doesn't exist
build:
	@echo "Checking for docker-compose.yaml..."
	@if [ ! -f docker-compose.yaml ]; then \
		echo "Downloading docker-compose.yaml..."; \
		curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.2/docker-compose.yaml'; \
	else \
		echo "docker-compose.yaml already exists."; \
	fi
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" > .env
	docker compose up airflow-init --build

# Start all Airflow services
deploy:
	docker compose up -d

stop:
	docker compose down

clean:
	docker compose down --volumes --rmi all
