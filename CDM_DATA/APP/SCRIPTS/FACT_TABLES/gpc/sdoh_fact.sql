create or replace sequence sdoh_text_index;
create or replace sequence sdoh_seq;

---SMOKING
create or replace table PRIVATE_SDOH_FACT as
select fact.*,
     pc.i2b2_patid                                               as I2B2_PATIENT_NUM,
     ec.i2b2_encounterid                                         as I2B2_ENCOUNTER_NUM,
     case 
       when smoking = '04' then  'LOINC:LA18978-9'
       when smoking = '05' then  'LOINC:LA18979-7'
       when smoking = '03' then  'LOINC:LA15920-4'
       when smoking = '01' then  'LOINC:LA18976-3'
       when smoking = '06' then  'LOINC:LA18980-5'
       when smoking = '02' then  'LOINC:LA18977-1'
       when smoking = '07' then  'LOINC:LA18981-3'
       when smoking = '08' then  'LOINC:LA18982-1'
       else 'LOINC:LA18980-5'
     end                                                            as I2B2_CONCEPT_CD,
     '@'                                                           as I2B2_PROVIDER_ID,
    MEASURE_DATE :: TIMESTAMP  as I2B2_START_DATE,
     '@'                                                            as I2B2_MODIFIER_CD,
     1                                                              as I2B2_INSTANCE_NUM,
     '@'                                                            as I2B2_VALTYPE_CD,
     null                                                           as I2B2_TVAL_CHAR,
     null                                                           as I2B2_NVAL_NUM,
     '@'                                                            as I2B2_VALUEFLAG_CD,
     null                                                           as I2B2_QUANTITY_NUM,
     '@'                                                            as I2B2_UNITS_CD,
     cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
     '@'                                                            as I2B2_LOCATION_CD,
     ''                                                             as I2B2_OBSERVATION_BLOB,
     null                                                           as I2B2_CONFIDENCE_NUM,
     CURRENT_TIMESTAMP                                                   as I2B2_UPDATE_DATE,
     CURRENT_TIMESTAMP                                                   as I2B2_DOWNLOAD_DATE,
     CURRENT_TIMESTAMP                                                   as I2B2_IMPORT_DATE,
     $cdm_version                                                   as I2B2_SOURCESYSTEM_CD,
     null                                                           as I2B2_UPLOAD_ID,
     sdoh_text_index.nextval                                        as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID);


create or replace table DEID_SDOH_FACT_T as
select t.*
     , sdoh_seq.nextval as TEXT_SEARCH_INDEX
from (
         select distinct I2B2_ENCOUNTER_NUM                                                                                                                         as ENCOUNTER_NUM,
                         I2B2_PATIENT_NUM                                                                                                                           as PATIENT_NUM,
                         cast( I2B2_CONCEPT_CD as VARCHAR(50))                                                                                                      as CONCEPT_CD,
                         cast(I2B2_PROVIDER_ID as VARCHAR(50))                                                                                                      as PROVIDER_ID,
                         I2B2_START_DATE                                                                                                                            as START_DATE,
                         cast(I2B2_MODIFIER_CD as VARCHAR(100))                                                                                                     as MODIFIER_CD,
                         row_number() over (
                              partition by ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD,PROVIDER_ID,START_DATE,MODIFIER_CD 
                              order by I2B2_TEXT_SEARCH_INDEX
                         )                                                                                                                                          as INSTANCE_NUM,
                         cast( I2B2_VALTYPE_CD as VARCHAR(50))                                                                                                      as VALTYPE_CD,
                         cast(I2B2_TVAL_CHAR as VARCHAR(255))                                                                                                       as TVAL_CHAR,
                         cast(I2B2_NVAL_NUM as DECIMAL(18,5))                                                                                                       as NVAL_NUM,
                         cast(I2B2_VALUEFLAG_CD as VARCHAR(50))                                                                                                     as VALUEFLAG_CD,
                         cast(I2B2_QUANTITY_NUM as DECIMAL(18, 5))                                                                                                  as QUANTITY_NUM,
                         cast(I2B2_UNITS_CD as VARCHAR(50))                                                                                                         as UNITS_CD,
                         cast(I2B2_END_DATE as TIMESTAMP)                                                                                                           as END_DATE,
                         cast(I2B2_LOCATION_CD as VARCHAR(50))                                                                                                      as LOCATION_CD,
                         cast(I2B2_OBSERVATION_BLOB as TEXT)                                                                                                        as OBSERVATION_BLOB,
                         cast(I2B2_CONFIDENCE_NUM as DECIMAL(18, 5))                                                                                                as CONFIDENCE_NUM,
                         I2B2_UPDATE_DATE                                                                                                                           as UPDATE_DATE,
                         I2B2_DOWNLOAD_DATE                                                                                                                         as DOWNLOAD_DATE,
                         I2B2_IMPORT_DATE                                                                                                                           as IMPORT_DATE,
                         cast(I2B2_SOURCESYSTEM_CD as VARCHAR(50))                                                                                                  as SOURCESYSTEM_CD,
                         cast(I2B2_UPLOAD_ID as integer)                                                                                                            AS UPLOAD_ID
         from PRIVATE_SDOH_FACT
         order by ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD
) as t;

insert into DEID_SDOH_FACT_T (                            
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
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
     END AS CONCEPT_CD,
     PROVIDER_ID, 
     coalesce(START_DATE, CURRENT_TIMESTAMP), 
     '@',
     1,
     '',
     '',
     null,
     '',
     null,
     '@',
     null,
     '@',
     '',
     null ,
     CURRENT_TIMESTAMP,
     CURRENT_TIMESTAMP,
     CURRENT_TIMESTAMP,                            
     $cdm_version, 
     null,
     sdoh_seq.nextval
  from DEID_VISIT_DIMENSION
;

create or replace view DEID_SDOH_FACT as
select * from DEID_SDOH_FACT_T;