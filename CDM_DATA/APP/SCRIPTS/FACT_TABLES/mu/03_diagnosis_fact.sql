create or replace view  #target_schema.DIAGNOSIS_FACT as
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                          as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                as PATIENT_NUM, 
    case
          when dx_type = '10' then concat('ICD10CM:', dx)
          when dx_type = '09' then 
               case --icd9 trailing '.' dots
                    when right(dx, 1) = '.' then  concat('ICD9CM:', split_part(dx, '.', 1))
                    else concat('ICD9CM:', dx)
               end
          else concat(dx_type, ':', dx)
     end                                                                                        as CONCEPT_CD,
    COALESCE(fact.PROVIDERID, '@')                                                              as PROVIDER_ID, 
    ADMIT_DATE :: TIMESTAMP                                                                     as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    cast(null as TIMESTAMP)                                                                     as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                           as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
from #source_schema.DEID_DIAGNOSIS fact;
