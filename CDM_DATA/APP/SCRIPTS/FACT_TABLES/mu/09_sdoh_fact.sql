create or replace view #target_schema.SDOH_FACT as
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    case 
     --   when smoking = '04' then  'LOINC:LA18978-9'
     --   when smoking = '05' then  'LOINC:LA18979-7'
     --   when smoking = '03' then  'LOINC:LA15920-4'
     --   when smoking = '01' then  'LOINC:LA18976-3'
     --   when smoking = '06' then 'LOINC:LA18980-5'
     --   when smoking = '02' then  'LOINC:LA18977-1'
     --   when smoking = '07' then  'LOINC:LA18981-3'
     --   when smoking = '08' then  'LOINC:LA18982-1'
     --   else 'LOINC:LA18980-5'
          when OBSCLIN_RESULT_TEXT = 'Never smoker' then  'LOINC:LA18978-9'
          when OBSCLIN_RESULT_TEXT = 'Smoker, current status unknown' then  'LOINC:LA18979-7'
          when OBSCLIN_RESULT_TEXT = 'Former smoker quit within 12 months' then  'LOINC:LA15920-4'
          when OBSCLIN_RESULT_TEXT = 'Former smoker quit longer than 12 months' then  'LOINC:LA15920-4'
          when OBSCLIN_RESULT_TEXT = 'Current every day smoker' then  'LOINC:LA18976-3'
          when OBSCLIN_RESULT_TEXT = 'Unknown if ever smoked' then 'LOINC:LA18980-5'
          when OBSCLIN_RESULT_TEXT = 'Current some day smoker' then  'LOINC:LA18977-1'
          when OBSCLIN_RESULT_TEXT = 'Heavy tobacco smoker' then  'LOINC:LA18981-3'
          when OBSCLIN_RESULT_TEXT = 'Light tobacco smoker' then  'LOINC:LA18982-1'
               else 'LOINC:LA18980-5'
     end                                                                                            as CONCEPT_CD,
    '@'                                                                                             as PROVIDER_ID, 
    TO_TIMESTAMP(fact.obsclin_start_date   :: DATE || ' ' || fact.obsclin_start_time, 'YYYY-MM-DD HH24:MI:SS')          as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('' as VARCHAR(50))                                                                         as VALTYPE_CD,
    cast('' as VARCHAR(255))                                                                        as TVAL_CHAR, 
    cast(null as DECIMAL(18, 5))                                                                    as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('@' as VARCHAR(50))                                                                        as UNITS_CD, 
    TO_TIMESTAMP(COALESCE(fact.obsclin_stop_date, fact.obsclin_start_date) :: DATE || ' ' || COALESCE(fact.obsclin_stop_time, '00:00:00'), 'YYYY-MM-DD HH24:MI:SS')  as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from #source_schema.DEID_OBS_CLIN fact 
where OBSCLIN_CODE = 'C29719'
union all
select
    ENCOUNTER_NUM, 
    PATIENT_NUM, 
    CASE
          WHEN PAYER_TYPE_PRIMARY =  '1' THEN  'LOINC:LA15652-3'
          WHEN PAYER_TYPE_PRIMARY =  '2' THEN  'LOINC:LA17849-3'
          WHEN PAYER_TYPE_PRIMARY =  '3' THEN  'LOINC:LA22076-6'
          WHEN PAYER_TYPE_PRIMARY =  '4' THEN  'LOINC:LA22076-6'
          WHEN PAYER_TYPE_PRIMARY =  '5' THEN  'LOINC:LA22073-3'
          WHEN PAYER_TYPE_PRIMARY =  '8' THEN  'LOINC:LA22078-2'
          WHEN PAYER_TYPE_PRIMARY =  '9' THEN  'LOINC:LA22079-0'
          WHEN PAYER_TYPE_PRIMARY =  'OT' THEN 'LOINC:LA22079-0'
          WHEN PAYER_TYPE_PRIMARY =  'UN' THEN 'LOINC:LA22079-0'
          ELSE 'LOINC:LA22079-0'
     END                                                                                            as CONCEPT_CD,
    PROVIDER_ID                                                                                     as PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_TIMESTAMP)                                                         as START_DATE,  
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('' as VARCHAR(50))                                                                         as VALTYPE_CD,
    cast('' as VARCHAR(255))                                                                        as TVAL_CHAR, 
    cast(null as DECIMAL(18, 5))                                                                    as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('@' as VARCHAR(50))                                                                        as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                         as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                                     as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from  #target_schema.VISIT_DIMENSION fact;