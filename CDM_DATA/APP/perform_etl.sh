#!/bin/bash

CONFIGURE_PATH=/app/db.properties
ESCAPED_URL=$(echo "$JDBC_URL" | sed 's/&/\\&/g')

sed -i "s|db.username=.*|db.username=$USERNAME|g" ${CONFIGURE_PATH}
sed -i "s|db.password=.*|db.password=$PASSWORD|g" ${CONFIGURE_PATH}
sed -i "s|db.url=.*|db.url=$ESCAPED_URL|g" ${CONFIGURE_PATH}
sed -i "s|db.project=.*|db.project=$PROJECT|g" ${CONFIGURE_PATH}

CONFIGURE_PATH=/app/SCRIPTS/OTHER/$PROJECT/project_config.sql
sed -i "s|#target_db|$TARGET_DB|g" ${CONFIGURE_PATH}
sed -i "s|#source_schema|$SOURCE_DB.$SOURCE_SCHEMA|g" ${CONFIGURE_PATH}
sed -i "s|#target_schema|$TARGET_SCHEMA|g" ${CONFIGURE_PATH}
sed -i "s|#pm_db|$PM_DB|g" ${CONFIGURE_PATH}


CONFIGURE_PATH=/app/SCRIPTS/OTHER/$PROJECT/crosswalk.sql
sed -i "s|#source_schema|$SOURCE_DB.$SOURCE_SCHEMA|g" ${CONFIGURE_PATH}
sed -i "s|#target_schema|$TARGET_DB.$TARGET_SCHEMA|g" ${CONFIGURE_PATH}


CONFIGURE_PATH=/app/SCRIPTS/OTHER/$PROJECT/delete.sql
sed -i "s|#target_db|$TARGET_DB|g" ${CONFIGURE_PATH}

find /app/SCRIPTS/DIMENSION_TABLES/$PROJECT/ -type f -exec sed -i "s|#source_schema|$SOURCE_DB.$SOURCE_SCHEMA|g" {} +
find /app/SCRIPTS/DIMENSION_TABLES/$PROJECT/ -type f -exec sed -i "s|#target_schema|$TARGET_DB.$TARGET_SCHEMA|g" {} +

find /app/SCRIPTS/FACT_TABLES/$PROJECT/ -type f -exec sed -i "s|#source_schema|$SOURCE_DB.$SOURCE_SCHEMA|g" {} +
find /app/SCRIPTS/FACT_TABLES/$PROJECT/ -type f -exec sed -i "s|#target_schema|$TARGET_DB.$TARGET_SCHEMA|g" {} +
find /app/SCRIPTS/FACT_TABLES/$PROJECT/ -type f -exec sed -i "s|#target_db|$TARGET_DB|g" {} +


# Perform ETL opertation
ant -f ./data_build.xml perform_etl

# prject_config
ant -f ./data_build.xml project_config

# Generate patient count in ontology
# ant -f ./data_build.xml cdm_count

# Generate missing obs ontology
# ant -f ./data_build.xml missing_obs