set patient_dimension = $target_schema || '.' || 'patient_dimension';
set provider_dimension = $target_schema || '.' || 'provider_dimension';
set visit_dimension = $target_schema || '.' || 'visit_dimension';

set observation_fact = $target_schema || '.' || 'observation_fact';
set demographic_fact = $target_schema || '.' || 'demographic_fact';
set diagnosis_fact = $target_schema || '.' || 'diagnosis_fact';
set lab_fact = $target_schema || '.' || 'lab_fact';
set procedure_fact = $target_schema || '.' || 'procedure_fact';
set prescribing_fact = $target_schema || '.' || 'prescribing_fact';
set vital_fact = $target_schema || '.' || 'vital_fact';
set visit_fact = $target_schema || '.' || 'visit_fact';
set OBSCLIN_FACT = $target_schema || '.' || 'obsclin_fact';
set COVID_FACT = $target_schema || '.' || 'covid_fact';
set SDOH_FACT = $target_schema || '.' || 'sdoh_fact';


--DIMENSION TABLES

--patient dimension
create or replace table identifier($patient_dimension) as
select * from DEID_PATIENT_DIMENSION;

--provider dimension
create or replace table identifier($provider_dimension) as
select * from DEID_PROVIDER_DIMENSION;

--visit dimension
create or replace table identifier($visit_dimension) as
select * from DEID_VISIT_DIMENSION;

----FACT TABLES-----

--observation fact
create or replace table identifier($observation_fact) as
select * from DEID_OBSERVATION_FACT;

--demographic fact
create or replace table identifier($demographic_fact) as
select * from DEID_DEMOGRAPHIC_FACT;

--
create or replace table identifier($diagnosis_fact) as
select * from DEID_DIAGNOSIS_FACT;

--lab fact
create or replace table identifier($lab_fact) as
select * from DEID_LAB_FACT;

--procedures fact
create or replace table identifier($procedure_fact) as
select * from DEID_PROCEDURE_FACT;

--prescribing fact
create or replace table identifier($prescribing_fact) as
select * from DEID_PRESCRIBING_FACT;


-- visit fact
create or replace table identifier($visit_fact) as
select * from DEID_VISIT_FACT;

-- vitals fact
create or replace table identifier($vital_fact) as
select * from DEID_VITAL_FACT;

-- covid fact
create or replace table identifier($COVID_FACT) as
select * from DEID_COVID_FACT;


-- sdoh fact
create or replace table identifier($SDOH_FACT) as
select * from DEID_SDOH_FACT;