CREATE OR REPLACE TABLE PRIVATE_VISIT_FACT LIKE PRIVATE_OBSERVATION_FACT;
create or replace sequence visit_text_index;

-- visit type
insert into PRIVATE_VISIT_FACT (
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    ENCOUNTER_NUM, 
    PATIENT_NUM, 
    concat('VISIT|TYPE:', COALESCE(inout_cd, 'UN')),
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_DATE()), 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    $cdm_version,                                                                    
    null, 
    visit_text_index.nextVal 
from DEID_VISIT_DIMENSION as dim;        



-- Length of stay
insert into PRIVATE_VISIT_FACT (                            
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    encounter_num, 
    PATIENT_NUM, 
    CASE
        WHEN LENGTH_OF_STAY =  0 THEN  concat('VISIT|LENGTH:', '0')
        WHEN LENGTH_OF_STAY =  1 THEN  concat('VISIT|LENGTH:', '1')
        WHEN LENGTH_OF_STAY =  2 THEN  concat('VISIT|LENGTH:', '2')
        WHEN LENGTH_OF_STAY =  3 THEN  concat('VISIT|LENGTH:', '3')
        WHEN LENGTH_OF_STAY =  4 THEN  concat('VISIT|LENGTH:', '4')
        WHEN LENGTH_OF_STAY =  5 THEN  concat('VISIT|LENGTH:', '5')
        WHEN LENGTH_OF_STAY =  6 THEN  concat('VISIT|LENGTH:', '6')
        WHEN LENGTH_OF_STAY =  7 THEN  concat('VISIT|LENGTH:', '7')
        WHEN LENGTH_OF_STAY =  8 THEN  concat('VISIT|LENGTH:', '8')
        WHEN LENGTH_OF_STAY =  9 THEN  concat('VISIT|LENGTH:', '9')
        WHEN LENGTH_OF_STAY =  10 THEN  concat('VISIT|LENGTH:', '10')
        WHEN LENGTH_OF_STAY >  10 THEN  concat('VISIT|LENGTH:', '>10')
        ELSE concat('VISIT|LENGTH:', '0')
    END,
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_DATE()), 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    $cdm_version,                                            
    null, 
    visit_text_index.nextVal 
from DEID_VISIT_DIMENSION as dim;

-- CREATE VIEW
create or replace view  DEID_VISIT_FACT as 
select * from PRIVATE_VISIT_FACT;         



