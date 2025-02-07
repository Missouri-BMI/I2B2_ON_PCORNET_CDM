create or replace view  I2B2_DEV.I2B2DATA.ADI_FACT as
-- create or replace view  {target_schema}.ADI_FACT as
select
    cast(-1 as NUMBER(38, 0))                                                                   as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                as PATIENT_NUM, 
    'ADI_NATRANK:' || ADI_NATRANK                                                               as CONCEPT_CD,
    '@'                                                                                         as PROVIDER_ID, 
    ADDRESS_PERIOD_START :: TIMESTAMP                                                           as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    ADDRESS_PERIOD_END :: TIMESTAMP                                                             as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                   as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
-- from {source_schema}.DEID_ADI fact
from DEIDENTIFIED_PCORNET_CDM.CDM.DEID_ADI fact
where ADI_NATRANK is not null
union all
select
    cast(-1 as NUMBER(38, 0))                                                                   as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                as PATIENT_NUM, 
    'ADI_STATERNK:' || ADI_STATERNK                                                             as CONCEPT_CD,
    '@'                                                                                         as PROVIDER_ID, 
    ADDRESS_PERIOD_START :: TIMESTAMP                                                           as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    ADDRESS_PERIOD_END :: TIMESTAMP                                                             as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                   as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
-- from {source_schema}.DEID_ADI fact
from DEIDENTIFIED_PCORNET_CDM.CDM.DEID_ADI fact
where ADI_STATERNK is not null;