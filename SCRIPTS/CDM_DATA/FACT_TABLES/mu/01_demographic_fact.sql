
-- CREATE VIEW
create or replace view {target_schema}.DEMOGRAPHIC_FACT as 
-- demographic hispanic
select
    cast(-1 as NUMBER(38, 0))                                                       as ENCOUNTER_NUM, 
    PATIENT_NUM, 
    concat('DEM|HISP:', COALESCE(dim.HISPANIC, 'NI'))                               as CONCEPT_CD,
    '@'                                                                             as PROVIDER_ID, 
    CURRENT_TIMESTAMP                                                               as START_DATE,  
    '@'                                                                             as MODIFIER_CD,
    1                                                                               as INSTANCE_NUM, 
    ''                                                                              as VALTYPE_CD,
    ''                                                                              as TVAL_CHAR, 
    cast(null as  integer)                                                          as NVAL_NUM, 
    ''                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                          as QUANTITY_NUM, 
    '@'                                                                             as UNITS_CD, 
    cast(null  as TIMESTAMP)                                                        as END_DATE, 
    '@'                                                                             as LOCATION_CD, 
    cast(null as  text)                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                          as UPLOAD_ID
from {target_schema}.PATIENT_DIMENSION as dim
union all 
-- demographic race
select
    cast(-1 as NUMBER(38, 0))                                                       as ENCOUNTER_NUM, 
    PATIENT_NUM, 
    CASE
        WHEN dim.RACE_CD =  'American Indian or Alaska Native' THEN  concat('DEM|RACE:', 'NA')
        WHEN dim.RACE_CD =  'Asian' THEN  concat('DEM|RACE:', 'AS')
        WHEN dim.RACE_CD =  'Black or African American' THEN  concat('DEM|RACE:', 'B')
        WHEN dim.RACE_CD =  'Native Hawaiian or Other Pacific Islander' THEN  concat('DEM|RACE:', 'H')
        WHEN dim.RACE_CD =  'White' THEN  concat('DEM|RACE:', 'W')
        WHEN dim.RACE_CD =  'Multiple race' THEN  concat('DEM|RACE:', 'M')
        WHEN dim.RACE_CD =  'Refuse to answer' THEN  concat('DEM|RACE:', '07')
        WHEN dim.RACE_CD =  'No information' THEN  concat('DEM|RACE:', 'NI')
        WHEN dim.RACE_CD =  'Unknown' THEN  concat('DEM|RACE:', 'UN')
        WHEN dim.RACE_CD =  'Other' THEN  concat('DEM|RACE:', 'OT')
        ELSE concat('DEM|RACE:', 'NI')
    END                                                                             as CONCEPT_CD,
    '@'                                                                             as PROVIDER_ID, 
    CURRENT_TIMESTAMP                                                               as START_DATE,  
    '@'                                                                             as MODIFIER_CD,
    1                                                                               as INSTANCE_NUM, 
    ''                                                                              as VALTYPE_CD,
    ''                                                                              as TVAL_CHAR, 
    cast(null as  integer)                                                          as NVAL_NUM, 
    ''                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                          as QUANTITY_NUM, 
    '@'                                                                             as UNITS_CD, 
    cast(null  as TIMESTAMP)                                                        as END_DATE, 
    '@'                                                                             as LOCATION_CD, 
    cast(null as  text)                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                          as UPLOAD_ID
from {target_schema}.PATIENT_DIMENSION as dim
union all 
--SEX
select
    cast(-1 as NUMBER(38, 0))                                                       as ENCOUNTER_NUM, 
    PATIENT_NUM, 
    concat('DEM|SEX:', COALESCE(dim.SEX_CD, 'NI'))                                  as CONCEPT_CD,
    '@'                                                                             as PROVIDER_ID, 
    CURRENT_TIMESTAMP                                                               as START_DATE,  
    '@'                                                                             as MODIFIER_CD,
    1                                                                               as INSTANCE_NUM, 
    ''                                                                              as VALTYPE_CD,
    ''                                                                              as TVAL_CHAR, 
    cast(null as  integer)                                                          as NVAL_NUM, 
    ''                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                          as QUANTITY_NUM, 
    '@'                                                                             as UNITS_CD, 
    cast(null  as TIMESTAMP)                                                        as END_DATE, 
    '@'                                                                             as LOCATION_CD, 
    cast(null as  text)                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                          as UPLOAD_ID
from {target_schema}.PATIENT_DIMENSION as dim
union all 
--VITAL STATUS
select
    cast(-1 as NUMBER(38, 0))                                                       as ENCOUNTER_NUM, 
    PATIENT_NUM, 
     CASE
        WHEN VITAL_STATUS_CD = 'Y' THEN 'DEM|VITAL STATUS:D'
        ELSE '@'
    END                                                                             as CONCEPT_CD,
    '@'                                                                             as PROVIDER_ID, 
    CURRENT_TIMESTAMP                                                               as START_DATE,  
    '@'                                                                             as MODIFIER_CD,
    1                                                                               as INSTANCE_NUM, 
    ''                                                                              as VALTYPE_CD,
    ''                                                                              as TVAL_CHAR, 
    cast(null as  integer)                                                          as NVAL_NUM, 
    ''                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                          as QUANTITY_NUM, 
    '@'                                                                             as UNITS_CD, 
    cast(null  as TIMESTAMP)                                                        as END_DATE, 
    '@'                                                                             as LOCATION_CD, 
    cast(null as  text)                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                          as UPLOAD_ID
from {target_schema}.PATIENT_DIMENSION as dim;
