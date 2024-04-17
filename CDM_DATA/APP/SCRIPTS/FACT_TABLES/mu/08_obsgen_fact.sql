create or replace view  #target_schema.OBSGEN_FACT as
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                          as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                as PATIENT_NUM, 
    concat('LOINC:', OBSGEN_CODE)                                                               as CONCEPT_CD,
    '@'                                                                                         as PROVIDER_ID, 
    TO_TIMESTAMP(OBSGEN_START_DATE   :: DATE || ' ' || OBSGEN_START_TIME, 'YYYY-MM-DD HH24:MI:SS')     as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    TO_TIMESTAMP(COALESCE(OBSGEN_STOP_DATE, OBSGEN_START_DATE) :: DATE || ' ' || COALESCE(OBSGEN_STOP_TIME, '00:00:00'), 'YYYY-MM-DD HH24:MI:SS')    as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                                  as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
from #source_schema.DEID_OBS_GEN fact
where OBSGEN_TYPE = 'LC'  and ENCOUNTERID is not null;