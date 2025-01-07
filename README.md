
# Usage Guide

This repository contains tools and workflows to facilitate data operations and analysis within the i2b2 on PCORnet common data models in Snowflake. Follow the steps below to set up and execute the necessary processes.


## Airflow Setup
Initialize and run Apache Airflow to manage the data workflows:
bash
```
./airflow-init
./airflow-run
```
## Airflow DAGs
The following DAGs (Directed Acyclic Graphs) are available to perform specific tasks:

### i2b2_data_install.py

**Purpose**: 
- Creates the i2b2 schema, tables, and common data using i2b2-data.

### i2b2_data_refresh.py

**Purpose**:
- Creates PCORnet fact views.
- Executes total count scripts.
- Generates analysis results.

### i2b2_generated_ont.py

**Purpose**: 
 - Creates and populates custom ontologies maintained by the University of Missouri (MU).
