## This module offers a range of functionalities, including
* Harmonizing PCORnet CDM data with i2b2 data.
* Setting up pipelines for loading PCORnet/GPC (Greater Plains Collaborative) data into i2b2 databases.
* Providing scripts for modifying and enriching ontologies.

### CDM refresh in i2b2
```sh
cd CDM_DATA
docker-compose --env-file ./env/dev/.env up --build
```
### EXAMPLE OF ENVIRONMENT VARIABLES
```sh
[mu/gpc]
PROJECT=mu
ACCOUNT=
USERNAME=
PASSWORD=
WAREHOUSE=I2B2_ETL_WH
ROLE=I2B2
UPDATE_DATE = "October 2023"
PROJECT_TEXT = "NextGen Data Lake De-Identified"

SOURCE_DB=DEIDENTIFIED_PCORNET_CDM
SOURCE_SCHEMA=CDM_2023_OCT

TARGET_DB=I2B2_DEV
TARGET_SCHEMA=I2B2DATA
PM_DB=I2B2_DEV
JDBC_URL=jdbc:snowflake://fp20843.us-east-2.aws.snowflakecomputing.com/?db=I2B2_DEV&schema=I2B2METADATA&warehouse=I2B2_ETL_WH&role=I2B2&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true

```


### ONTOLOGY refresh in i2b2

- EXPORTER: Export and download i2b2 ontology tables in csv from snowflake
- IMPORT: Import i2b2 ontology tables in csv format from local filesystem to Snowflake
- READ_SCRIPTS: Load act_ontology_v4 postgresql tables in csv format to snowflake
- UPDATE_SCRIPTS: Modify loaded postgresql version of act_ontology_v4 in snowflake to support snowflake SQL engine and PCORNET



## ENACT 4.1
https://www.ctsiredcap.pitt.edu/redcap/surveys/?s=9PLLPXMX9LYPHMFA