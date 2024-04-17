--- 
CREATE OR REPLACE SEQUENCE  #target_schema.PATIENT_SEQ START = 1 INCREMENT = 1;

CREATE OR REPLACE TABLE #target_schema.patient_crosswalk (
    patient_num NUMBER(38, 0) DEFAULT #target_schema.PATIENT_SEQ.NEXTVAL,
    patid VARCHAR(701) not null
);

INSERT into #target_schema.patient_crosswalk  (patid)
select distinct patid from #source_schema.V_DEID_DEMOGRAPHIC;

--
CREATE OR REPLACE SEQUENCE  #target_schema.ENCOUNTER_SEQ START = 1 INCREMENT = 1;
CREATE OR REPLACE TABLE #target_schema.encounter_crosswalk (
    encounter_num NUMBER(38, 0) DEFAULT #target_schema.ENCOUNTER_SEQ.NEXTVAL,
    ENCOUNTERID  VARCHAR(701) not null
);

INSERT into #target_schema.encounter_crosswalk  (ENCOUNTERID)
select distinct ENCOUNTERID from #source_schema.V_DEID_ENCOUNTER;
