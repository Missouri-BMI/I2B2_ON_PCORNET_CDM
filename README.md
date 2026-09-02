# Usage Guide

This repository contains tools and workflows to facilitate data operations within the i2b2 and PCORnet common datamodel in Snowflake. Follow the steps below to set up and execute the necessary processes.

#### Add Airflow Connections

To configure Airflow for Snowflake environments, create a `connections.sh` script in the `env/` directory:
Structure for managing environment variables in local directory:
The `env/` directory is structured to manage environment-specific configuration files and secrets for both deidentified and identified data environments. Example structure:

```
env/
  [deidentified, identified]/
    [dev, prod, sandbox]/
        mu.env,gpc.env,mu-id.env              # Environment variables for deidentified dev
    connections.sh        # Airflow connection script 
    rsa_key.p8            # Private key for Snowflake authentication
```
- Place your environment variable files (e.g., `.env`) in the appropriate subdirectory (`dev` for development, `prod` for production).
- Store your Snowflake private key as `rsa_key.p8` in the corresponding directory.
- `make airflow-i2b2-connections` runs each account's `connections.sh` inside the
  `airflow-apiserver` container. Each script builds a single JSON file and loads it
  with [`airflow connections import ... --overwrite`](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html),
  which boots the provider manager once (instead of once per connection) and makes
  the target **idempotent** — re-running overwrites existing connections instead of failing.

connections.sh
```bash
#!/bin/bash
set -euo pipefail

# -----------------------------
# Global Snowflake parameters
# -----------------------------

USERNAME="I2B2_ETL_USER"
ACCOUNT="TKNLTGA-I2B2DB"
WAREHOUSE="I2B2_ETL_WH"
ROLE="I2B2"
PRIVATE_KEY_FILE="/opt/airflow/env/deidentified/rsa_key.p8"
PASSWORD="your-pass"

# -----------------------------
# Connection catalog: "conn_id|database"
# -----------------------------
CONNECTIONS=(
  # Sandbox
  "snowflake_conn_deidentified_sandbox_mu|I2B2_ETL_TEST"
  # Dev
  "snowflake_conn_deidentified_dev_mu|I2B2_DEV"
  "snowflake_conn_deidentified_dev_shrine_mu|I2B2_SHRINE_MU_DEV"
  "snowflake_conn_deidentified_dev_shrine_washu|I2B2_SHRINE_WASHU_DEV"
  # Prod
  "snowflake_conn_deidentified_prod_mu|I2B2_PROD"
  "snowflake_conn_deidentified_prod_gpc|I2B2_SANDBOX_GPC"
  "snowflake_conn_deidentified_prod_shrine_mu|I2B2_SHRINE_MU_PROD"
  "snowflake_conn_deidentified_prod_shrine_washu|I2B2_SHRINE_WASHU_PROD"
)

# -----------------------------
# Build a single import file and load it in one pass.
# `extra_dejson` is serialized into the connection's `extra` on import.
# -----------------------------
CONN_FILE="$(mktemp "${TMPDIR:-/tmp}/snowflake_connections.XXXXXX.json")"
trap 'rm -f "$CONN_FILE"' EXIT

export USERNAME PASSWORD ACCOUNT WAREHOUSE ROLE PRIVATE_KEY_FILE

python3 - "$CONN_FILE" "${CONNECTIONS[@]}" <<'PY'
import json, os, sys

out_file, *pairs = sys.argv[1:]

common = {
    "conn_type": "snowflake",
    "login": os.environ["USERNAME"],
    "password": os.environ["PASSWORD"],
}
extra = {
    "account": os.environ["ACCOUNT"],
    "warehouse": os.environ["WAREHOUSE"],
    "role": os.environ["ROLE"],
    "private_key_file": os.environ["PRIVATE_KEY_FILE"],
}

connections = {}
for pair in pairs:
    conn_id, database = pair.split("|", 1)
    connections[conn_id] = {**common, "extra_dejson": {**extra, "database": database}}

with open(out_file, "w") as fh:
    json.dump(connections, fh, indent=2)
PY

echo "🔧 Importing ${#CONNECTIONS[@]} Snowflake connections..."
airflow connections import "$CONN_FILE" --overwrite

echo "✅ All Airflow Snowflake connections created successfully!"
```

To add a connection, add one `"conn_id|database"` line to the `CONNECTIONS` array.

#### Set Environment Variables

Example `mu.env` file:

```env
CONNECTION_ID=snowflake_conn_deidentified_dev_mu

PROJECT=mu
PROJECT_DB=I2B2_DEV

SOURCE_DB=DEIDENTIFIED_PCORNET_CDM
SOURCE_SCHEMA=CDM

TARGET_DB=I2B2_DEV
TARGET_SCHEMA=I2B2DATA

CRC_SCHEMA=I2B2DATA
HIVE_SCHEMA=I2B2HIVE
METADATA_SCHEMA=I2B2METADATA
PM_SCHEMA=I2B2PM
WORKDATA_SCHEMA=I2B2WORKDATA
```
### Pipeline

The `Makefile` provides commands to streamline setup and management of the OMOP on PCORnet CDM environment:

- **Build Stack**:  
  `airflow-i2b2-build` checks for `docker-compose.yaml` and downloads it from the official Apache Airflow documentation if missing.

- **Start Airflow Services**:  
  `airflow-i2b2-deploy` starts all Airflow services using Docker Compose.

- **Create Airflow Connections**:  
  `airflow-i2b2-connections` imports all Airflow database connections from each
  account's `connections.sh`. Safe to re-run — existing connections are overwritten.
