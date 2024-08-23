create or replace view  {target_schema}.MEDICATION_FACT as
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    concat('RXNORM:', RXNORM_CUI)                                                                   as CONCEPT_CD,
    COALESCE(fact.RX_PROVIDERID, '@')                                                               as PROVIDER_ID, 
    TO_TIMESTAMP(RX_ORDER_DATE   :: DATE || ' ' || RX_ORDER_TIME, 'YYYY-MM-DD HH24:MI:SS')          as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('' as VARCHAR(50))                                                                         as VALTYPE_CD,
    cast('' as VARCHAR(255))                                                                        as TVAL_CHAR, 
    cast(null as DECIMAL(18, 5))                                                                    as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('@' as VARCHAR(50))                                                                        as UNITS_CD, 
    RX_END_DATE :: TIMESTAMP                                                                        as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.DEID_PRESCRIBING fact
where RXNORM_CUI is not null;