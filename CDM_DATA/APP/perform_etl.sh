#!/bin/bash

CONFIGURE_PATH=/app/db.properties
ESCAPED_URL=$(echo "$JDBC_URL" | sed 's/&/\\&/g')

sed -i "s|db.username=.*|db.username=$USERNAME|g" ${CONFIGURE_PATH}
sed -i "s|db.password=.*|db.password=$PASSWORD|g" ${CONFIGURE_PATH}
sed -i "s|db.url=.*|db.url=$ESCAPED_URL|g" ${CONFIGURE_PATH}
sed -i "s|db.project=.*|db.project=$PROJECT|g" ${CONFIGURE_PATH}

CONFIGURE_PATH=/app/SCRIPTS/OTHER/$PROJECT/init.sql
sed -i "s|#cdm_db|$CDM_DB|g" ${CONFIGURE_PATH}
sed -i "s|#cdm_version|$CDM_VERSION|g" ${CONFIGURE_PATH}
sed -i "s|#intermediate_db|$INTERMEDIATE_DB|g" ${CONFIGURE_PATH}
sed -i "s|#target_db|$TARGET_DB|g" ${CONFIGURE_PATH}
sed -i "s|#target_schema|$TARGET_SCHEMA|g" ${CONFIGURE_PATH}


sed -i "s|#death_table|$DEATH_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#patient_table|$PATIENT_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#provider_table|$PROVIDER_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#visit_table|$VISIT_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#diagnosis_table|$DIAGNOSIS_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#lab_table|$LAB_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#obs_clin_table|$OBS_CLIN_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#obs_gen_table|$OBS_GEN_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#medication_table|$MEDICATION_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#procedures_table|$PROCEDURES_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#vital_table|$VITAL_TABLE|g" ${CONFIGURE_PATH}


sed -i "s|#patient_crosswalk|$PATIENT_CROSSWALK|g" ${CONFIGURE_PATH}
sed -i "s|#encounter_crosswalk|$ENCOUNTER_CROSSWALK|g" ${CONFIGURE_PATH}

CONFIGURE_PATH=/app/SCRIPTS/OTHER/$PROJECT/cleanup.sql

sed -i "s|#update_date|$UPDATE_DATE|g" ${CONFIGURE_PATH}
sed -i "s|#project_text|$PROJECT_TEXT|g" ${CONFIGURE_PATH}


# Perform ETL opertation
ant -f ./data_build.xml perform_etl

# Generate patient count in ontology
ant -f ./data_build.xml cdm_count

# Generate missing obs ontology
ant -f ./data_build.xml missing_obs