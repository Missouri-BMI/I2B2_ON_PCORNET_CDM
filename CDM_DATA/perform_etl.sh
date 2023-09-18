#!/bin/bash

CONFIGURE_PATH=/app/CDM_ETL/db.properties
sed -i "s|#USERNAME|$USERNAME|g" ${CONFIGURE_PATH}
sed -i "s|#PASSWORD|$PASSWORD|g" ${CONFIGURE_PATH}
sed -i "s|#JDBC_URL|$JDBC_URL|g" ${CONFIGURE_PATH}


CONFIGURE_PATH=/app/CDM_ETL/SCRIPTS/init.sql
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
sed -i "s|#medication_table|$MEDICATION_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#procedures_table|$PROCEDURES_TABLE|g" ${CONFIGURE_PATH}
sed -i "s|#vital_table|$VITAL_TABLE|g" ${CONFIGURE_PATH}


sed -i "s|#patient_crosswalk|$PATIENT_CROSSWALK|g" ${CONFIGURE_PATH}
sed -i "s|#encounter_crosswalk|$ENCOUNTER_CROSSWALK|g" ${CONFIGURE_PATH}

# Perform ETL opertation
ant -f ./CDM_ETL/data_build.xml perform_etl

# Generate patient count in ontology
# python3 ./CDM_COUNT/run_all_counts.py

# Generate gap in CDM vs Ontologies
# python3 ./ETL_VALIDATOR/validator.py