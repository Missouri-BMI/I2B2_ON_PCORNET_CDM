create or replace view  #target_schema.PROCEDURE_FACT as
select
    ec.ENCOUNTER_NUM                                                                                as ENCOUNTER_NUM, 
    pc.PATIENT_NUM                                                                                  as PATIENT_NUM, 
    case 
        when px_type = '10' then concat('ICD10PCS:',px)
        when px_type = '09' then concat('ICD9PROC:',px)
        when px_type = 'CH' then concat('CPT4',':', px)
        else concat(px_type,':',px)
    end                                                                                         as CONCEPT_CD,
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
    cast(null  	as TIMESTAMP)                                                                   as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                   as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
from #source_schema.PCORNET_DEID_PROCEDURES fact
inner join #target_schema.patient_crosswalk as pc
using (patid)
inner join #target_schema.encounter_crosswalk as ec
using (ENCOUNTERID);  
