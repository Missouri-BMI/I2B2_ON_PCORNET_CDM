## Usage

## Repositories
```
git clone https://github.com/Missouri-BMI/I2B2_ON_PCORNET_CDM
git clone https://github.com/Missouri-BMI/I2B2_SERVERLESS
```

## New Installation

### Create database schemas
```
BEGIN
    CREATE DATABASE IF NOT EXISTS I2B2_DB;
    USE DATABASE I2B2_DB;
    
    CREATE OR REPLACE SCHEMA I2B2DATA;
    CREATE OR REPLACE SCHEMA I2B2METADATA;
    CREATE OR REPLACE SCHEMA I2B2HIVE;
    CREATE OR REPLACE SCHEMA I2B2PM;
    CREATE OR REPLACE SCHEMA I2B2WORKDATA;
END;
```

### Create database schema tables and prepopulated data from i2b2 data installer
```

cd I2B2_SERVERLESS/Docker/data-installer
change database connection properties in config/ path
./docker-build # sh docker-build
./docker-run
```

### Load Snowflake version of ACT Ontology 4.1 mapped with pcornet cdm

```
cd I2B2_ON_P_CDM/ONTOLOGY_TABLES
modfiy conf/snowsql.cnf
./docker-build
./docker-run
```
## Refresh i2b2 databa:

### Load i2b2 data
```
cd I2B2_ON_P_CDM/CDM_DATA
put your db configure file in the env directory
docker-compose --env-file ./env/dev/.env up --build
```
