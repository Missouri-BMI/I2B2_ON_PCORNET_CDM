create or replace view  {target_schema}.VITAL_FACT as
---HT
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    'LOINC:8302-2'                                                                                  as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS')              as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('N' as VARCHAR(50))                                                                        as VALTYPE_CD,
    'E'                                                                                             as TVAL_CHAR, 
    COALESCE(cast(fact.HT as DECIMAL(18, 5)), null)                                                 as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('inches' as VARCHAR(50))                                                                   as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_VITAL fact 
where ENCOUNTERID is not null
union all
----WT
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    'LOINC:3141-9'                                                                                  as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS')              as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('N' as VARCHAR(50))                                                                        as VALTYPE_CD,
    'E'                                                                                             as TVAL_CHAR, 
    COALESCE(cast(fact.WT as DECIMAL(18, 5)), null)                                                 as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('kg' as VARCHAR(50))                                                                       as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_VITAL fact
where ENCOUNTERID is not null
union all
---DIASTOLIC
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    'LOINC:8462-4'                                                                                  as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS')              as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('N' as VARCHAR(50))                                                                        as VALTYPE_CD,
    'E'                                                                                             as TVAL_CHAR, 
    COALESCE(cast(fact.DIASTOLIC as DECIMAL(18, 5)), null)                                          as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('mm Hg' as VARCHAR(50))                                                                    as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_VITAL fact
where ENCOUNTERID is not null
union all
---SYSTOLIC
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    'LOINC:8480-6'                                                                                  as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS')              as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('N' as VARCHAR(50))                                                                        as VALTYPE_CD,
    'E'                                                                                             as TVAL_CHAR, 
    COALESCE(cast(fact.SYSTOLIC as DECIMAL(18, 5)), null)                                           as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('mm Hg' as VARCHAR(50))                                                                    as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_VITAL fact
where ENCOUNTERID is not null
union all
---BMI
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    'LOINC:39156-5'                                                                                 as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS')              as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('N' as VARCHAR(50))                                                                        as VALTYPE_CD,
    'E'                                                                                             as TVAL_CHAR, 
    COALESCE(cast(fact.ORIGINAL_BMI as DECIMAL(18, 5)), null)                                       as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('kg/m2' as VARCHAR(50))                                                                    as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_VITAL fact
where ENCOUNTERID is not null;