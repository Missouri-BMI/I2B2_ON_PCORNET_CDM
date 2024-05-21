create or replace view  #target_schema.OBSGEN_FACT as
select
    cast(COALESCE(ENCOUNTERID, -1) as NUMBER(38, 0))                                            as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                as PATIENT_NUM, 
    concat(raw_obsgen_type,':',raw_obsgen_name)                                                 as CONCEPT_CD,
    '@'                                                                                         as PROVIDER_ID, 
    OBSGEN_START_DATE                                                                           as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    OBSGEN_STOP_DATE                                                                            as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                                  as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
from #source_schema.DEID_OBS_GEN_SDOH_ACS fact
where OBSGEN_TYPE = 'OT';