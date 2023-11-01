set cdm_db = '#cdm_db';
set cdm_version = '#cdm_version';
set cdm_schema = $cdm_db || '.' || $cdm_version;
set i2b2_intermediate_db = '#intermediate_db';
set target_db = '#target_db';
set pm_db = '#pm_db';
set target_schema = $target_db || '.' || '#target_schema';

--dimensions
set source_death = $cdm_schema || '.#death_table';
set patient_source_table = $cdm_schema || '.#patient_table';
set provider_source_table = $cdm_schema || '.#provider_table';
set visit_source_table = $cdm_schema || '.#visit_table';

--facts
set diagnosis_source_table = $cdm_schema || '.#diagnosis_table';
set lab_source_table = $cdm_schema || '.#lab_table';
set obs_clin_source_table = $cdm_schema || '.#obs_clin_table';
set obs_gen_source_table = $cdm_schema || '.#obs_gen_table';

set prescribing_source_table = $cdm_schema || '.#medication_table';
set procedures_source_table = $cdm_schema || '.#procedures_table';
set vital_source_table = $cdm_schema || '.#vital_table';

set patient_crosswalk = '#patient_crosswalk';
set encounter_crosswalk = '#encounter_crosswalk';

create database if not exists identifier($i2b2_intermediate_db);
use database identifier($i2b2_intermediate_db);

create schema if not exists identifier($cdm_version);
use schema identifier($cdm_version);

--- 
CREATE OR REPLACE SEQUENCE  PATIENT_SEQ START = 1 INCREMENT = 1;

CREATE OR REPLACE TABLE identifier($patient_crosswalk) (
    i2b2_patid NUMBER DEFAULT PATIENT_SEQ.NEXTVAL,
    patid VARCHAR(701) not null
);

INSERT into identifier($patient_crosswalk)  (patid)
select distinct patid from identifier($patient_source_table);

--
CREATE OR REPLACE SEQUENCE  ENCOUNTER_SEQ START = 1 INCREMENT = 1;
CREATE OR REPLACE TABLE identifier($encounter_crosswalk) (
    i2b2_encounterid NUMBER DEFAULT ENCOUNTER_SEQ.NEXTVAL,
    ENCOUNTERID  VARCHAR(701) not null
);

INSERT into identifier($encounter_crosswalk)  (ENCOUNTERID)
select distinct ENCOUNTERID from identifier($visit_source_table);
