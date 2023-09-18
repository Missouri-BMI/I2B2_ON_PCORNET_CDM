### Add new ETL script

-> Add fact table ETL script
-> include in the RUNTIME/data_build.xm
-> add included table placeholder inthe .env and dockercompose
-> add variables in perform_etl.sh

### gpc / MU changes 
    - .env
    - cleanup.sql
    - data-migration.sql
    - data_build.xml
    - patient_dimension
    - procedure_fact


## Run
docker-compose --env-file ./env/dev/.env up --build
