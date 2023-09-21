use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace sequence visit_text_index;

-- visit type
create or replace view VISIT_FACT
as
select
    ENCOUNTER_NUM as ENCOUNTER_NUM, 
    PATIENT_NUM as PATIENT_NUM, 
    concat('VISIT|TYPE:', COALESCE(inout_cd, 'UN')) as CONCEPT_CD,
    PROVIDER_ID as PROVIDER_ID, 
    CURRENT_DATE() as START_DATE, 
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM, 
    '' as VALTYPE_CD,
    '' as TVAL_CHAR, 
    null as NVAL_NUM, 
    '' as VALUEFLAG_CD,
    null as QUANTITY_NUM, 
    '@' as UNITS_CD, 
    null as END_DATE, 
    '@' as LOCATION_CD, 
    null as OBSERVATION_BLOB, 
    null as CONFIDENCE_NUM, 
    CURRENT_DATE() as UPDATE_DATE,
    CURRENT_DATE() as DOWNLOAD_DATE,
    CURRENT_DATE() as IMPORT_DATE,
    'CDM 6.0' as SOURCESYSTEM_CD,                                                                    
    null AS UPLOAD_ID, 
    visit_text_index.nextVal as TEXT_SEARCH_INDEX
from DEID_VISIT_DIMENSION as dim
UNION ALL
-- Length of stay
select
    encounter_num as ENCOUNTER_NUM, 
    PATIENT_NUM as PATIENT_NUM, 
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
    END as CONCEPT_CD,
    PROVIDER_IDas PROVIDER_ID, 
    CURRENT_DATE() as START_DATE, 
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM, 
    '' as VALTYPE_CD,
    '' as TVAL_CHAR, 
    null as NVAL_NUM, 
    '' as VALUEFLAG_CD,
    null as QUANTITY_NUM, 
    '@' as UNITS_CD, 
    null as END_DATE, 
    '@' as LOCATION_CD, 
    null as OBSERVATION_BLOB, 
    null as CONFIDENCE_NUM, 
    CURRENT_DATE() as UPDATE_DATE,
    CURRENT_DATE() as DOWNLOAD_DATE,
    CURRENT_DATE() as IMPORT_DATE,
    'CDM 6.0' as SOURCESYSTEM_CD,                                                                    
    null AS UPLOAD_ID, 
    visit_text_index.nextVal  as TEXT_SEARCH_INDEX
from DEID_VISIT_DIMENSION as dim;

     



