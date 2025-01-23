# Usage Guide

This repository contains tools and workflows to facilitate data operations within the i2b2 and PCORnet common datamodel in Snowflake. Follow the steps below to set up and execute the necessary processes.

## Airflow Setup

Initialize and run Apache Airflow to manage the data workflows:
```
./airflow-init
./airflow-run
```

## Airflow DAGs

The following DAGs (Directed Acyclic Graphs) are available to perform specific tasks:

### snowflake_i2b2
**Purpose**:
- Stage and transform i2b2 ENACT data in Snowflake.
- Modify Postgresql SQL queries to be compatible with Snowflake.
- Create zip files and export them to the i2b2-data repository.

### i2b2_data_install
**Purpose**:
 - Creates the i2b2 schema, tables, and common data conifugrations using i2b2-data repository.

### i2b2_data_refresh
**Purpose**:
- Creates PCORnet fact views.
- Executes total count scripts.
- Generates analysis results.
- Update i2b2 project data per refresh.

### i2b2_generated_ont
**Purpose**:
- Creates and populates custom ontologies maintained by the University of Missouri (MU).

## Configuration Variables for the Project:
```
# General Configuration
# PROJECT: The name of the project.
# Example: mu
PROJECT=

# ACCOUNT: The Snowflake account identifier, typically including the account name, region, and cloud provider.
# Example: fp20843.us-east-2.aws
ACCOUNT=

# USERNAME: The username for Snowflake authentication.
# Example: I2B2_ETL_USER
USERNAME=

# PASSWORD: The password for Snowflake authentication.
# Note: Use a secure method to store and retrieve passwords.
PASSWORD=

# WAREHOUSE: The name of the Snowflake warehouse to be used for queries.
# Example: I2B2_ETL_WH
WAREHOUSE=

# ROLE: The role assigned to the Snowflake user.
# Example: I2B2
ROLE=

# PROJECT_DB: The database associated with the project in Snowflake.
# Example: I2B2_DEV
PROJECT_DB=

# Source Database Configuration
# SOURCE_DB: The source database name in Snowflake.
# Example: DEIDENTIFIED_PCORNET_CDM
SOURCE_DB=

# SOURCE_SCHEMA: The schema within the source database.
# Example: CDM
SOURCE_SCHEMA=

# Target Database Configuration
# TARGET_DB: The target database name in Snowflake.
# Example: I2B2_DEV
TARGET_DB=

# TARGET_SCHEMA: The schema within the target database.
# Example: I2B2DATA
TARGET_SCHEMA=

# Additional Schema Configuration
# CRC_SCHEMA: The schema for clinical research data.
# Example: I2B2DATA
CRC_SCHEMA=

# HIVE_SCHEMA: The schema for I2B2 hive data.
# Example: I2B2HIVE
HIVE_SCHEMA=

# METADATA_SCHEMA: The schema for I2B2 metadata.
# Example: I2B2METADATA
METADATA_SCHEMA=

# PM_SCHEMA: The schema for project management data.
# Example: I2B2PM
PM_SCHEMA=

# WORKDATA_SCHEMA: The schema for work-related data.
# Example: I2B2WORKDATA
WORKDATA_SCHEMA=

# JDBC Configuration
# JDBC_URL: The JDBC connection string for Snowflake.
# Example: jdbc:snowflake://<ACCOUNT>.snowflakecomputing.com/?db=<TARGET_DB>&schema=<METADATA_SCHEMA>&warehouse=<WAREHOUSE>&role=<ROLE>&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true&JDBC_QUERY_RESULT_FORMAT=JSON

JDBC_URL=jdbc:snowflake://$ACCOUNT.snowflakecomputing.com/?db=$TARGET_DB&schema=$METADATA_SCHEMA&warehouse=$WAREHOUSE&role=$ROLE&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true&JDBC_QUERY_RESULT_FORMAT=JSON
```