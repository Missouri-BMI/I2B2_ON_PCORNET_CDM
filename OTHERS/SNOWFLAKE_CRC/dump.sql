set search_path to i2b2hive;
update crc_db_lookup
set c_db_servertype = 'SNOWFLAKE', c_db_fullschema = 'I2B2DATA';


-- change metadata schema name

set search_path to i2b2demodata;
drop table diagnosis_fact;
drop table lab_fact;
drop table archive_observation_fact;
drop table code_lookup;
drop table datamart_report;
drop table encounter_mapping;
drop table lab_fact;
drop table concept_dimension;
drop table modifier_dimension;
drop table observation_fact;
drop table patient_dimension;
drop table patient_mapping;
drop table prescribing_fact;
drop table procedure_fact;
drop table provider_dimension;
drop table visit_dimension;

