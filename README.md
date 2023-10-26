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
ACCOUNT=<account_id>.us-east-2.aws
USERNAME=
PASSWORD=
WAREHOUSE=I2B2_ETL_WH
ROLE=I2B2

CDM_DB=DEIDENTIFIED_PCORNET_CDM
CDM_VERSION=CDM_2023_JULY
INTERMEDIATE_DB=I2B2_PCORNET_CDM

TARGET_DB=I2B2_DEV
PM_DB=I2B2_DEV
TARGET_SCHEMA=I2B2DATA

DEATH_TABLE=DEID_DEATH
PATIENT_TABLE=DEID_DEMOGRAPHIC
PROVIDER_TABLE=DEID_PROVIDER
VISIT_TABLE=DEID_ENCOUNTER

DIAGNOSIS_TABLE=DEID_DIAGNOSIS
LAB_TABLE=DEID_LAB_RESULT_CM
OBS_CLIN_TABLE=DEID_OBS_CLIN
OBS_GEN_TABLE=DEID_OBS_GEN
MEDICATION_TABLE=DEID_PRESCRIBING
PROCEDURES_TABLE=DEID_PROCEDURES
VITAL_TABLE=DEID_VITAL

PATIENT_CROSSWALK=PATIENT_CROSSWALK
ENCOUNTER_CROSSWALK=ENCOUNTER_CROSSWALK

JDBC_URL=jdbc:snowflake://<account_id>.us-east-2.aws.snowflakecomputing.com/?db=I2B2_DEV&schema=I2B2METADATA&warehouse=I2B2_ETL_WH&role=I2B2&CLIENT_RESULT_COLUMN_CASE_INSENSITIVE=true

```


### ONTOLOGY refresh in i2b2

- EXPORTER: Export and download i2b2 ontology tables in csv from snowflake
- IMPORT: Import i2b2 ontology tables in csv format from local filesystem to Snowflake
- READ_SCRIPTS: Load act_ontology_v4 postgresql tables in csv format to snowflake
- UPDATE_SCRIPTS: Modify loaded postgresql version of act_ontology_v4 in snowflake to support snowflake SQL engine and PCORNET