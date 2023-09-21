use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace sequence prescribing_seq;

create or replace view PRIVATE_PRESCRIBING_FACT
as
select fact.*,
     pc.i2b2_patid                                as I2B2_PATIENT_NUM,
     ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
       concat('RXNORM:', RXNORM_CUI)       as I2B2_CONCEPT_CD,
       COALESCE(fact.RX_PROVIDERID, '-1') as I2B2_PROVIDER_ID,
       cast('@' as VARCHAR(100))          as I2B2_MODIFIER_CD,
       cast(1 as integer)                 as I2B2_INSTANCE_NUM,
       cast('' as VARCHAR(50))            as I2B2_VALTYPE_CD,
       cast('' as VARCHAR(255))           as I2B2_TVAL_CHAR,
       cast(null as DECIMAL(18, 5))       as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))            as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))       as I2B2_QUANTITY_NUM,
       cast('@' as VARCHAR(50))           as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)            as I2B2_END_DATE,
       cast('@' as VARCHAR(50))           as I2B2_LOCATION_CD,
       cast('' as TEXT)                   as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))       as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)            as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)            as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)            as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))         as I2B2_SOURCESYSTEM_CD,       
       cast(null as integer)              as I2B2_UPLOAD_ID
from DEIDENTIFIED_PCORNET_CDM.CDM_2023_JULY.DEID_PRESCRIBING fact
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.PATIENT_CROSSWALK as pc
using (patid)
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.ENCOUNTER_CROSSWALK as ec
using (ENCOUNTERID);         

create or replace view PRESCRIBING_FACT as
select *
     , prescribing_seq.nextval as TEXT_SEARCH_INDEX
from (
         select distinct I2B2_ENCOUNTER_NUM           as ENCOUNTER_NUM,
                         I2B2_PATIENT_NUM             as PATIENT_NUM,
                         I2B2_CONCEPT_CD              as CONCEPT_CD,
                         I2B2_PROVIDER_ID             as PROVIDER_ID,
                         RX_ORDER_DATE                as START_DATE,
                         I2B2_MODIFIER_CD             as MODIFIER_CD,
                         I2B2_INSTANCE_NUM            as INSTANCE_NUM,
                         I2B2_VALTYPE_CD              as VALTYPE_CD,
                         I2B2_TVAL_CHAR               as TVAL_CHAR,
                         I2B2_NVAL_NUM                as NVAL_NUM,
                         I2B2_VALUEFLAG_CD            as VALUEFLAG_CD,
                         I2B2_QUANTITY_NUM            as QUANTITY_NUM,
                         I2B2_UNITS_CD                as UNITS_CD,
                         I2B2_END_DATE                as END_DATE,
                         I2B2_LOCATION_CD             as LOCATION_CD,
                         I2B2_OBSERVATION_BLOB        as OBSERVATION_BLOB,
                         I2B2_CONFIDENCE_NUM          as CONFIDENCE_NUM,
                         I2B2_UPDATE_DATE             as UPDATE_DATE,
                         I2B2_DOWNLOAD_DATE           as DOWNLOAD_DATE,
                         I2B2_IMPORT_DATE             as IMPORT_DATE,
                         I2B2_SOURCESYSTEM_CD         as SOURCESYSTEM_CD,
                         I2B2_UPLOAD_ID               AS UPLOAD_ID
         from PRIVATE_PRESCRIBING_FACT    
         where concept_cd <> 'RXNORM:'
     ) as t;

       