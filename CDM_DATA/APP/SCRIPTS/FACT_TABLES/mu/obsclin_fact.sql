create or replace sequence obs_clin_text_index;
create or replace sequence obs_clin_seq;

create or replace table PRIVATE_OBSCLIN_FACT
as
select fact.*,
    pc.i2b2_patid                                as I2B2_PATIENT_NUM,
    ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
    CASE 
        when obsclin_type = 'LC' then concat('LOINC:', obsclin_code)
        when obsclin_type = 'SM' then concat('SNOMED:', obsclin_code)
        else concat('OT:', obsclin_code)
        END                                 as I2B2_CONCEPT_CD,
    '-1'                               as I2B2_PROVIDER_ID,
    cast('@' as VARCHAR(100))          as I2B2_MODIFIER_CD,
    cast(1 as integer)                 as I2B2_INSTANCE_NUM,
    cast('N' as VARCHAR(50))           as I2B2_VALTYPE_CD,
    CASE
        when obsclin_result_modifier = 'LT' then 'L'
        when obsclin_result_modifier = 'EQ' then 'E'
        when obsclin_result_modifier = 'GT' then 'G'
        else 'E'
        END                            as I2B2_TVAL_CHAR,
    cast(obsclin_RESULT_NUM as DECIMAL(18, 5)) as I2B2_NVAL_NUM,
    cast('' as VARCHAR(50))            as I2B2_VALUEFLAG_CD,
    cast(null as DECIMAL(18, 5))       as I2B2_QUANTITY_NUM,
    cast(obsclin_RESULT_UNIT as VARCHAR(50))   as I2B2_UNITS_CD,
    cast(null as TIMESTAMP)            as I2B2_END_DATE,
    cast('@' as VARCHAR(50))           as I2B2_LOCATION_CD,
    cast('' as TEXT)                   as I2B2_OBSERVATION_BLOB,
    cast(null as DECIMAL(18, 5))       as I2B2_CONFIDENCE_NUM,
    cast(null as TIMESTAMP)            as I2B2_UPDATE_DATE,
    cast(null as TIMESTAMP)            as I2B2_DOWNLOAD_DATE,
    cast(null as TIMESTAMP)            as I2B2_IMPORT_DATE,
    cast($cdm_version as VARCHAR(50))  as I2B2_SOURCESYSTEM_CD,    
    cast(null as integer)              as I2B2_UPLOAD_ID,
    obs_clin_text_index.nextval                 AS I2B2_TEXT_SEARCH_INDEX
from identifier($obs_clin_source_table) fact   
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)
where obsclin_result_modifier <> 'TX'
UNION
select fact.*,
    pc.i2b2_patid                                as I2B2_PATIENT_NUM,
    ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
    CASE 
        when obsclin_type = 'LC' then concat('LOINC:', obsclin_code)
        when obsclin_type = 'SM' then concat('SNOMED:', obsclin_code)
        else concat('OT:', obsclin_code)
        END                                 as I2B2_CONCEPT_CD,
    '-1'                              as I2B2_PROVIDER_ID,
    cast('@' as VARCHAR(100))         as I2B2_MODIFIER_CD,
    cast(1 as integer)                as I2B2_INSTANCE_NUM,
    cast('T' as VARCHAR(50))          as I2B2_VALTYPE_CD,
    cast(OBSCLIN_RESULT_QUAL as VARCHAR(255)) as I2B2_TVAL_CHAR,
    cast(null as DECIMAL(18, 5))      as I2B2_NVAL_NUM,
    cast('' as VARCHAR(50))           as I2B2_VALUEFLAG_CD,
    cast(null as DECIMAL(18, 5))      as I2B2_QUANTITY_NUM,
    cast(OBSCLIN_RESULT_UNIT as VARCHAR(50))  as I2B2_UNITS_CD,
    cast(null as TIMESTAMP)           as I2B2_END_DATE,
    cast('@' as VARCHAR(50))          as I2B2_LOCATION_CD,
    cast('' as TEXT)                  as I2B2_OBSERVATION_BLOB,
    cast(null as DECIMAL(18, 5))      as I2B2_CONFIDENCE_NUM,
    cast(null as TIMESTAMP)           as I2B2_UPDATE_DATE,
    cast(null as TIMESTAMP)           as I2B2_DOWNLOAD_DATE,
    cast(null as TIMESTAMP)           as I2B2_IMPORT_DATE,
    cast($cdm_version as VARCHAR(50))        as I2B2_SOURCESYSTEM_CD,     
    cast(null as integer)             as I2B2_UPLOAD_ID,
    obs_clin_text_index.nextval                 AS I2B2_TEXT_SEARCH_INDEX
from identifier($obs_clin_source_table) fact  
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)   
where obsclin_result_modifier = 'TX'
;

create or replace table DEID_OBSCLIN_FACT_T as          
select *
     , obs_clin_seq.nextval as TEXT_SEARCH_INDEX
from (
         select distinct I2B2_ENCOUNTER_NUM                                                                                                                     as ENCOUNTER_NUM,
                         I2B2_PATIENT_NUM                                                                                                                        as PATIENT_NUM,
                         I2B2_CONCEPT_CD                                                                                                                           as CONCEPT_CD,
                         I2B2_PROVIDER_ID                                                                                                                          as PROVIDER_ID,
                         OBSCLIN_DATE                                                                                                                              as START_DATE,
                         I2B2_MODIFIER_CD                                                                                                                          as MODIFIER_CD,
                         row_number() over (partition by ENCOUNTER_NUM, patient_num,CONCEPT_CD,PROVIDER_ID,START_DATE,MODIFIER_CD order by i2b2_text_search_index) as INSTANCE_NUM,
                         I2B2_VALTYPE_CD                                                                                                                           as VALTYPE_CD,
                         I2B2_TVAL_CHAR                                                                                                                            as TVAL_CHAR,
                         I2B2_NVAL_NUM                                                                                                                             as NVAL_NUM,
                         I2B2_VALUEFLAG_CD                                                                                                                         as VALUEFLAG_CD,
                         I2B2_QUANTITY_NUM                                                                                                                         as QUANTITY_NUM,
                         I2B2_UNITS_CD                                                                                                                             as UNITS_CD,
                         I2B2_END_DATE                                                                                                                             as END_DATE,
                         I2B2_LOCATION_CD                                                                                                                          as LOCATION_CD,
                         I2B2_OBSERVATION_BLOB                                                                                                                     as OBSERVATION_BLOB,
                         I2B2_CONFIDENCE_NUM                                                                                                                       as CONFIDENCE_NUM,
                         I2B2_UPDATE_DATE                                                                                                                          as UPDATE_DATE,
                         I2B2_DOWNLOAD_DATE                                                                                                                        as DOWNLOAD_DATE,
                         I2B2_IMPORT_DATE                                                                                                                          as IMPORT_DATE,
                         I2B2_SOURCESYSTEM_CD                                                                                                                      as SOURCESYSTEM_CD,
                         I2B2_UPLOAD_ID                                                                                                                            AS UPLOAD_ID
         from PRIVATE_OBSCLIN_FACT
         where (CONCEPT_CD <> 'LOINC:' and CONCEPT_CD <> 'SNOMED:')
         order by ENCOUNTER_NUM, patient_num, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD
     ) as t;

-- create view from temp table
create or replace view DEID_OBSCLIN_FACT as
select * from DEID_OBSCLIN_FACT_T; 