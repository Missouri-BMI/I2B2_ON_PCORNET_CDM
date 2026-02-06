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
- connection scripts run on make build and store in the db using api
- Store your Snowflake private key as `rsa_key.p8` in the corresponding directory.

connection.sh
```
#!/bin/bash
set -euo pipefail

# -----------------------------
# Global Snowflake parameters
# -----------------------------

USERNAME="I2B2_ETL_USER"
ACCOUNT="TKNLTGA-I2B2DB"
WAREHOUSE="I2B2_ETL_WH"
ROLE="I2B2"
PRIVATE_KEY_FILE="/opt/airflow/env/rsa_key.p8"
PASSWORD="your-pass"

# -----------------------------
# Helper function
# -----------------------------
create_snowflake_conn () {
  local conn_id="$1"
  local database="$2"

  echo "🔧 Creating Airflow connection: ${conn_id}"

  airflow connections add "${conn_id}" \
    --conn-type "snowflake" \
    --conn-login "${USERNAME}" \
    --conn-password "${PASSWORD}" \
    # --conn-schema "${CDM_SCHEMA}" \
    --conn-extra "{
      \"account\": \"${ACCOUNT}\",
      \"database\": \"${database}\",
      \"warehouse\": \"${WAREHOUSE}\",
      \"role\": \"${ROLE}\",
      \"private_key_file\": \"${PRIVATE_KEY_FILE}\"
    }"
}


# -----------------------------
# Sandbox
# -----------------------------
create_snowflake_conn \
  "snowflake_conn_deidentified_sandbox_mu" \
  "I2B2_ETL_TEST"

# -----------------------------
# Dev
# -----------------------------
create_snowflake_conn \
  "snowflake_conn_deidentified_dev_mu" \
  "I2B2_DEV"

create_snowflake_conn \
  "snowflake_conn_deidentified_dev_shrine_mu" \
  "I2B2_SHRINE_MU_DEV"

create_snowflake_conn \
  "snowflake_conn_deidentified_dev_shrine_washu" \
  "I2B2_SHRINE_WASHU_DEV"

# -----------------------------
# Prod
# -----------------------------
create_snowflake_conn \
  "snowflake_conn_deidentified_prod_mu" \
  "I2B2_PROD"

create_snowflake_conn \
  "snowflake_conn_deidentified_prod_gpc" \
  "I2B2_SANDBOX_GPC"

create_snowflake_conn \
  "snowflake_conn_deidentified_prod_shrine_mu" \
  "I2B2_SHRINE_MU_PROD"


create_snowflake_conn \
  "snowflake_conn_deidentified_prod_shrine_washu" \
  "I2B2_SHRINE_WASHU_PROD"


echo "✅ All Airflow Snowflake connections created successfully!"

```

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
  `airflow-i2b2-connections` create all Airflow database connections.
