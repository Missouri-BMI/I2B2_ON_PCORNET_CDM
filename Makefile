# Set the current user's UID for Airflow
AIRFLOW_UID := $(shell id -u)

# Download docker-compose.yaml if it doesn't exist
airflow-i2b2-build:
	@echo "Checking for docker-compose.yaml..."
	@if [ ! -f docker-compose.yaml ]; then \
		echo "Downloading docker-compose.yaml..."; \
		curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.6/docker-compose.yaml'; \
	else \
		echo "docker-compose.yaml already exists."; \
	fi
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" > .env
	docker compose up airflow-init --build

# Start all Airflow services
airflow-i2b2-deploy:
	docker compose up -d

airflow-i2b2-connections:
	@echo "Creating datasource connections..."
	docker compose exec airflow-apiserver bash -c '\
		set -euo pipefail; \
		BASE_DIR="/opt/airflow/env"; \
		echo "=== Installing Airflow connections (Snowflake) ==="; \
		for acct_dir in "$$BASE_DIR"/*/; do \
			acct="$$(basename "$$acct_dir")"; \
			script="$$acct_dir/connections.sh"; \
			if [ -f "$$script" ]; then \
				echo ""; \
				echo ">>> Running connection script for account: $$acct"; \
				bash "$$script"; \
			else \
				echo "!!! Skipping $$acct (connections.sh not found)"; \
			fi; \
		done; \
		echo ""; \
		echo "✅ Done installing all connections." \
	'

airflow-i2b2-stop:
	docker compose down

airflow-i2b2-clean:
	docker compose down --volumes --rmi all


pgadmin-start:
	@docker run -d --name pgadmin \
  -p 5050:80 \
  -e PGADMIN_DEFAULT_EMAIL="mhmcb@umsystem.edu" \
  -e PGADMIN_DEFAULT_PASSWORD="password" \
  -v pgadmin_data:/var/lib/pgadmin \
  --restart unless-stopped \
  dpage/pgadmin4:latest


pgadmin-stop:
	docker stop pgadmin
	docker rm pgadmin