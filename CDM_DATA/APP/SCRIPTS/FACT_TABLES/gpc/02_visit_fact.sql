create or replace view #target_schema.VISIT_FACT as
-- visit type
select
    ENCOUNTER_NUM, 
    PATIENT_NUM, 
    concat('VISIT|TYPE:', COALESCE(inout_cd, 'UN'))                                             as CONCEPT_CD,
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_TIMESTAMP)                                                     as START_DATE,  
    '@'                                                                                         as MODIFIER_CD,
    1                                                                                           as INSTANCE_NUM, 
    ''                                                                                          as VALTYPE_CD,
    ''                                                                                          as TVAL_CHAR, 
    cast(null as  integer)                                                                      as NVAL_NUM, 
    ''                                                                                          as VALUEFLAG_CD,
    cast(null as  integer)                                                                      as QUANTITY_NUM, 
    '@'                                                                                         as UNITS_CD, 
    coalesce(END_DATE, null :: TIMESTAMP)                                                       as END_DATE, 
    '@'                                                                                         as LOCATION_CD, 
    cast(null as  text)                                                                         as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                      as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                           as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                           as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                           as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                                  as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                      as UPLOAD_ID
from #target_schema.VISIT_DIMENSION as dim
union all 
-- Length of stay
select
    ENCOUNTER_NUM, 
    PATIENT_NUM, 
    CASE
        WHEN LENGTH_OF_STAY =  0 THEN  concat('VISIT|LENGTH:', '0')
        WHEN LENGTH_OF_STAY =  1 THEN  concat('VISIT|LENGTH:', '1')
        WHEN LENGTH_OF_STAY =  2 THEN  concat('VISIT|LENGTH:', '2')
        WHEN LENGTH_OF_STAY =  3 THEN  concat('VISIT|LENGTH:', '3')
        WHEN LENGTH_OF_STAY =  4 THEN  concat('VISIT|LENGTH:', '4')
        WHEN LENGTH_OF_STAY =  5 THEN  concat('VISIT|LENGTH:', '5')
        WHEN LENGTH_OF_STAY =  6 THEN  concat('VISIT|LENGTH:', '6')
        WHEN LENGTH_OF_STAY =  7 THEN  concat('VISIT|LENGTH:', '7')
        WHEN LENGTH_OF_STAY =  8 THEN  concat('VISIT|LENGTH:', '8')
        WHEN LENGTH_OF_STAY =  9 THEN  concat('VISIT|LENGTH:', '9')
        WHEN LENGTH_OF_STAY =  10 THEN  concat('VISIT|LENGTH:', '10')
        WHEN LENGTH_OF_STAY >  10 THEN  concat('VISIT|LENGTH:', '>10')
        ELSE concat('VISIT|LENGTH:', '0')
    END                                                                                        as CONCEPT_CD,
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_TIMESTAMP)                                                    as START_DATE,  
    '@'                                                                                        as MODIFIER_CD,
    1                                                                                          as INSTANCE_NUM, 
    ''                                                                                         as VALTYPE_CD,
    ''                                                                                         as TVAL_CHAR, 
    cast(null as  integer)                                                                     as NVAL_NUM, 
    ''                                                                                         as VALUEFLAG_CD,
    cast(null as  integer)                                                                     as QUANTITY_NUM, 
    '@'                                                                                        as UNITS_CD, 
    coalesce(END_DATE, null :: TIMESTAMP)                                                      as END_DATE, 
    '@'                                                                                        as LOCATION_CD, 
    cast(null as  text)                                                                        as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                     as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                          as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                          as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                          as IMPORT_DATE,
    cast( null as VARCHAR(50))                                                                as SOURCESYSTEM_CD,
    cast(null as  integer)                                                                     as UPLOAD_ID
from #target_schema.VISIT_DIMENSION as dim;
