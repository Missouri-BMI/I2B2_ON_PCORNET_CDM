use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace sequence demographic_text_index;


create or replace view DEMOGRAPHIC_FACT
as 
-- demographic hispanic
select
	'-1' as ENCOUNTER_NUM,
    dim.PATIENT_NUM  as PATIENT_NUM, 
    concat('DEM|HISP:', COALESCE(dim.HISPANIC, 'NI')) as CONCEPT_CD,
    '-1' as PROVIDER_ID, 
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
    demographic_text_index.nextVal  as TEXT_SEARCH_INDEX
from PATIENT_DIMENSION as dim
union
-- demographic race
select
    -1 as ENCOUNTER_NUM, 
    dim.PATIENT_NUM as PATIENT_NUM, 
    CASE
        WHEN dim.RACE_CD =  'American Indian or Alaska Native' THEN  concat('DEM|RACE:', '01')
        WHEN dim.RACE_CD =  'Asian' THEN  concat('DEM|RACE:', '02')
        WHEN dim.RACE_CD =  'Black or African American' THEN  concat('DEM|RACE:', '03')
        WHEN dim.RACE_CD =  'Native Hawaiian or Other Pacific Islander' THEN  concat('DEM|RACE:', '04')
        WHEN dim.RACE_CD =  'White' THEN  concat('DEM|RACE:', '05')
        WHEN dim.RACE_CD =  'Multiple race' THEN  concat('DEM|RACE:', '06')
        WHEN dim.RACE_CD =  'Refuse to answer' THEN  concat('DEM|RACE:', '07')
        WHEN dim.RACE_CD =  'No information' THEN  concat('DEM|RACE:', 'NI')
        WHEN dim.RACE_CD =  'Unknown' THEN  concat('DEM|RACE:', 'UN')
        WHEN dim.RACE_CD =  'Other' THEN  concat('DEM|RACE:', 'OT')
        ELSE concat('DEM|RACE:', 'NI')
    END as CONCEPT_CD,
    '-1' as PROVIDER_ID, 
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
    null as UPLOAD_ID, 
    demographic_text_index.nextVal as TEXT_SEARCH_INDEX
from DEID_PATIENT_DIMENSION as dim
union
select
    -1 as ENCOUNTER_NUM, 
    dim.PATIENT_NUM as PATIENT_NUM, 
    concat('DEM|SEX:', COALESCE(dim.SEX_CD, 'NI')) as CONCEPT_CD,
    '-1' as PROVIDER_ID, 
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
    null as UPLOAD_ID, 
    demographic_text_index.nextVal as TEXT_SEARCH_INDEX
from DEID_PATIENT_DIMENSION as dim
union
select
    -1 as ENCOUNTER_NUM, 
    dim.PATIENT_NUM as PATIENT_NUM, 
    CASE
        WHEN VITAL_STATUS_CD = 'Y' THEN 'DEM|VITAL STATUS:D'
        ELSE '@'
    END as CONCEPT_CD,
    '-1' as PROVIDER_ID, 
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
    null as UPLOAD_ID, 
    demographic_text_index.nextVal as TEXT_SEARCH_INDEX
from DEID_PATIENT_DIMENSION as dim; 

-- delete from PRIVATE_DEMOGRAPHIC_FACT where concept_cd = '@';




