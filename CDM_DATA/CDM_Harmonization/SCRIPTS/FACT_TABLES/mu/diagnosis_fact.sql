use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace sequence DIAGNOSIS_SEQ; 

create or replace view PRIVATE_DIAGNOSIS_FACT
as
select fact.*,
     pc.i2b2_patid                                as I2B2_PATIENT_NUM,
     ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
     case
          when dx_type = '10' then concat('ICD10CM:', dx)
          when dx_type = '09' then 
               case --icd9 trailing '.' dots
                    when right(dx, 1) = '.' then  concat('ICD9CM:', split_part(dx, '.', 1))
                    else concat('ICD9CM:', dx)
               end
          else concat(dx_type, ':', dx)
     end                                          as I2B2_CONCEPT_CD,
     COALESCE(fact.PROVIDERID, '-1')              as I2B2_PROVIDER_ID,
     cast('@' as VARCHAR(100))       as I2B2_MODIFIER_CD,
     cast(1 as integer)              as I2B2_INSTANCE_NUM,
     cast('' as VARCHAR(50))         as I2B2_VALTYPE_CD,
     cast('' as VARCHAR(255))        as I2B2_TVAL_CHAR,
     cast(null as DECIMAL(18, 5))    as I2B2_NVAL_NUM,
     cast('' as VARCHAR(50))         as I2B2_VALUEFLAG_CD,
     cast(null as DECIMAL(18, 5))    as I2B2_QUANTITY_NUM,
     cast('@' as VARCHAR(50))        as I2B2_UNITS_CD,
     cast(null as TIMESTAMP)         as I2B2_END_DATE,
     cast('@' as VARCHAR(50))        as I2B2_LOCATION_CD,
     cast('' as TEXT)                as I2B2_OBSERVATION_BLOB,
     cast(null as DECIMAL(18, 5))    as I2B2_CONFIDENCE_NUM,
     cast(null as TIMESTAMP)         as I2B2_UPDATE_DATE,
     cast(null as TIMESTAMP)         as I2B2_DOWNLOAD_DATE,
     cast(null as TIMESTAMP)         as I2B2_IMPORT_DATE,
     cast($cdm_version as VARCHAR(50))      as I2B2_SOURCESYSTEM_CD,        
     cast(null as integer)           as I2B2_UPLOAD_ID
from DEIDENTIFIED_PCORNET_CDM.CDM_2023_JULY.DEID_DIAGNOSIS fact
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.PATIENT_CROSSWALK as pc
using (patid)
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.ENCOUNTER_CROSSWALK as ec
using (ENCOUNTERID);         

-- temporary
-- delete from PRIVATE_DIAGNOSIS_FACT where length(dx) = 1;

                       
-- create temp table with sequence
create or replace view DEID_DIAGNOSIS_FACT_T as        
select *
     , DIAGNOSIS_SEQ.nextval as TEXT_SEARCH_INDEX
from (
         select distinct I2B2_ENCOUNTER_NUM                 as ENCOUNTER_NUM,
                         I2B2_PATIENT_NUM                   as PATIENT_NUM,
                         I2B2_CONCEPT_CD                    as CONCEPT_CD,
                         I2B2_PROVIDER_ID                   as PROVIDER_ID,
                         ADMIT_DATE                         as START_DATE,
                         I2B2_MODIFIER_CD                   as MODIFIER_CD,
                         I2B2_INSTANCE_NUM                  as INSTANCE_NUM,
                         I2B2_VALTYPE_CD                    as VALTYPE_CD,
                         I2B2_TVAL_CHAR                     as TVAL_CHAR,
                         I2B2_NVAL_NUM                      as NVAL_NUM,
                         I2B2_VALUEFLAG_CD                  as VALUEFLAG_CD,
                         I2B2_QUANTITY_NUM                  as QUANTITY_NUM,
                         I2B2_UNITS_CD                      as UNITS_CD,
                         I2B2_END_DATE                      as END_DATE,
                         I2B2_LOCATION_CD                   as LOCATION_CD,
                         I2B2_OBSERVATION_BLOB              as OBSERVATION_BLOB,
                         I2B2_CONFIDENCE_NUM                as CONFIDENCE_NUM,
                         I2B2_UPDATE_DATE                   as UPDATE_DATE,
                         I2B2_DOWNLOAD_DATE                 as DOWNLOAD_DATE,
                         I2B2_IMPORT_DATE                   as IMPORT_DATE,
                         I2B2_SOURCESYSTEM_CD               as SOURCESYSTEM_CD,
                         I2B2_UPLOAD_ID                     AS UPLOAD_ID

         from DIAGNOSIS_FACT
     ) as t;
   

