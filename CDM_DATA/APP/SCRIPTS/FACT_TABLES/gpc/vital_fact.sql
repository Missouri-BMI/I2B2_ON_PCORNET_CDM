create or replace sequence vital_text_index;
create or replace sequence vital_seq;

create or replace table PRIVATE_VITAL_FACT
as
---HT
select fact.*,
          pc.i2b2_patid                                as I2B2_PATIENT_NUM,
          ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
          'LOINC:8302-2'                               as I2B2_CONCEPT_CD,
       '@'                                            as I2B2_PROVIDER_ID,
      --  TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as I2B2_START_DATE,
         MEASURE_DATE :: TIMESTAMP  as I2B2_START_DATE,
       cast('@' as VARCHAR(100))                                    as I2B2_MODIFIER_CD,
       cast(1 as integer)                                           as I2B2_INSTANCE_NUM,
       cast('N' as VARCHAR(50))                                     as I2B2_VALTYPE_CD,
       'E'                                                          as I2B2_TVAL_CHAR,
       COALESCE(cast(fact.HT as DECIMAL(18, 5)), null)              as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))                                      as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))                                 as I2B2_QUANTITY_NUM,
       cast('inches' as VARCHAR(50))                                as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
       cast('@' as VARCHAR(50))                                     as I2B2_LOCATION_CD,
       cast('' as TEXT)                                             as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))                                 as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)                                      as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))                            as I2B2_SOURCESYSTEM_CD,
       cast(null as integer)                                        as I2B2_UPLOAD_ID,
       vital_text_index.nextval                                     as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)
union all
----WT
select fact.*,
          pc.i2b2_patid                                as I2B2_PATIENT_NUM,
          ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
          'LOINC:3141-9'                               as I2B2_CONCEPT_CD,
       '@'                                            as I2B2_PROVIDER_ID,
      --  TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as I2B2_START_DATE,
       MEASURE_DATE :: TIMESTAMP as I2B2_START_DATE,
       cast('@' as VARCHAR(100))                                    as I2B2_MODIFIER_CD,
       cast(1 as integer)                                           as I2B2_INSTANCE_NUM,
       cast('N' as VARCHAR(50))                                     as I2B2_VALTYPE_CD,
       'E'                                                          as I2B2_TVAL_CHAR,
       COALESCE(cast(fact.WT as DECIMAL(18, 5)), null)              as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))                                      as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))                                 as I2B2_QUANTITY_NUM,
       cast('kg' as VARCHAR(50))                                    as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
       cast('@' as VARCHAR(50))                                     as I2B2_LOCATION_CD,
       cast('' as TEXT)                                             as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))                                 as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)                                      as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))                            as I2B2_SOURCESYSTEM_CD,
       cast(null as integer)                                        as I2B2_UPLOAD_ID,
       vital_text_index.nextval                                     as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)
union all
---DIASTOLIC
select fact.*,
          pc.i2b2_patid                                as I2B2_PATIENT_NUM,
          ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
          'LOINC:8462-4'                               as I2B2_CONCEPT_CD,
       '@'                                            as I2B2_PROVIDER_ID,
      --  TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as I2B2_START_DATE,
       MEASURE_DATE :: TIMESTAMP as I2B2_START_DATE,
       cast('@' as VARCHAR(100))                                    as I2B2_MODIFIER_CD,
       cast(1 as integer)                                           as I2B2_INSTANCE_NUM,
       cast('N' as VARCHAR(50))                                     as I2B2_VALTYPE_CD,
       'E'                                                          as I2B2_TVAL_CHAR,
       COALESCE(cast(fact.DIASTOLIC as DECIMAL(18, 5)), null)       as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))                                      as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))                                 as I2B2_QUANTITY_NUM,
       cast('mm Hg' as VARCHAR(50))                                 as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
       cast('@' as VARCHAR(50))                                     as I2B2_LOCATION_CD,
       cast('' as TEXT)                                             as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))                                 as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)                                      as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))                            as I2B2_SOURCESYSTEM_CD,
       cast(null as integer)                                        as I2B2_UPLOAD_ID,
       vital_text_index.nextval                                     as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)
union all
---SYSTOLIC
select fact.*,
          pc.i2b2_patid                                as I2B2_PATIENT_NUM,
          ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
          'LOINC:8480-6'                               as I2B2_CONCEPT_CD,
       '@'                                            as I2B2_PROVIDER_ID,
      --  TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as I2B2_START_DATE,
      MEASURE_DATE :: TIMESTAMP as I2B2_START_DATE,
       cast('@' as VARCHAR(100))                                    as I2B2_MODIFIER_CD,
       cast(1 as integer)                                           as I2B2_INSTANCE_NUM,
       cast('N' as VARCHAR(50))                                     as I2B2_VALTYPE_CD,
       'E'                                                          as I2B2_TVAL_CHAR,
       COALESCE(cast(fact.SYSTOLIC as DECIMAL(18, 5)), null)       as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))                                      as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))                                 as I2B2_QUANTITY_NUM,
       cast('mm Hg' as VARCHAR(50))                                 as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
       cast('@' as VARCHAR(50))                                     as I2B2_LOCATION_CD,
       cast('' as TEXT)                                             as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))                                 as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)                                      as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))                            as I2B2_SOURCESYSTEM_CD,
       cast(null as integer)                                        as I2B2_UPLOAD_ID,
       vital_text_index.nextval                                     as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID)
union all
---BMI
select fact.*,
          pc.i2b2_patid                                as I2B2_PATIENT_NUM,
          ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
          'LOINC:39156-5'                               as I2B2_CONCEPT_CD,
       '@'                                            as I2B2_PROVIDER_ID,
      --  TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as I2B2_START_DATE,
       MEASURE_DATE :: TIMESTAMP as I2B2_START_DATE,
       cast('@' as VARCHAR(100))                                    as I2B2_MODIFIER_CD,
       cast(1 as integer)                                           as I2B2_INSTANCE_NUM,
       cast('N' as VARCHAR(50))                                     as I2B2_VALTYPE_CD,
       'E'                                                          as I2B2_TVAL_CHAR,
       COALESCE(cast(fact.ORIGINAL_BMI as DECIMAL(18, 5)), null)    as I2B2_NVAL_NUM,
       cast('' as VARCHAR(50))                                      as I2B2_VALUEFLAG_CD,
       cast(null as DECIMAL(18, 5))                                 as I2B2_QUANTITY_NUM,
       cast('kg/m2' as VARCHAR(50))                                 as I2B2_UNITS_CD,
       cast(null as TIMESTAMP)                                      as I2B2_END_DATE,
       cast('@' as VARCHAR(50))                                     as I2B2_LOCATION_CD,
       cast('' as TEXT)                                             as I2B2_OBSERVATION_BLOB,
       cast(null as DECIMAL(18, 5))                                 as I2B2_CONFIDENCE_NUM,
       cast(null as TIMESTAMP)                                      as I2B2_UPDATE_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_DOWNLOAD_DATE,
       cast(null as TIMESTAMP)                                      as I2B2_IMPORT_DATE,
       cast($cdm_version as VARCHAR(50))                            as I2B2_SOURCESYSTEM_CD,
       cast(null as integer)                                        as I2B2_UPLOAD_ID,
       vital_text_index.nextval                                     as I2B2_TEXT_SEARCH_INDEX
from identifier($vital_source_table) fact 
inner join identifier($patient_crosswalk) as pc
using (patid)
inner join identifier($encounter_crosswalk) as ec
using (ENCOUNTERID);


create or replace table DEID_VITAL_FACT_T as
select *
     , vital_seq.nextval as TEXT_SEARCH_INDEX
from (
         select distinct I2B2_ENCOUNTER_NUM                                                                                                                         as ENCOUNTER_NUM,
                         I2B2_PATIENT_NUM                                                                                                                           as PATIENT_NUM,
                         I2B2_CONCEPT_CD                                                                                                                            as CONCEPT_CD,
                         I2B2_PROVIDER_ID                                                                                                                           as PROVIDER_ID,
                         I2B2_START_DATE                                                                                                                            as START_DATE,
                         I2B2_MODIFIER_CD                                                                                                                           as MODIFIER_CD,
                         row_number() over (
                              partition by ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD,PROVIDER_ID,START_DATE,MODIFIER_CD 
                              order by I2B2_TEXT_SEARCH_INDEX
                         )                                                                                                                                          as INSTANCE_NUM,
                         I2B2_VALTYPE_CD                                                                                                                            as VALTYPE_CD,
                         I2B2_TVAL_CHAR                                                                                                                             as TVAL_CHAR,
                         I2B2_NVAL_NUM                                                                                                                              as NVAL_NUM,
                         I2B2_VALUEFLAG_CD                                                                                                                          as VALUEFLAG_CD,
                         I2B2_QUANTITY_NUM                                                                                                                          as QUANTITY_NUM,
                         I2B2_UNITS_CD                                                                                                                              as UNITS_CD,
                         I2B2_END_DATE                                                                                                                              as END_DATE,
                         I2B2_LOCATION_CD                                                                                                                           as LOCATION_CD,
                         I2B2_OBSERVATION_BLOB                                                                                                                      as OBSERVATION_BLOB,
                         I2B2_CONFIDENCE_NUM                                                                                                                        as CONFIDENCE_NUM,
                         I2B2_UPDATE_DATE                                                                                                                           as UPDATE_DATE,
                         I2B2_DOWNLOAD_DATE                                                                                                                         as DOWNLOAD_DATE,
                         I2B2_IMPORT_DATE                                                                                                                           as IMPORT_DATE,
                         I2B2_SOURCESYSTEM_CD                                                                                                                       as SOURCESYSTEM_CD,
                         I2B2_UPLOAD_ID                                                                                                                             AS UPLOAD_ID
         from PRIVATE_VITAL_FACT
         order by ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD
     ) as t;

create or replace view DEID_VITAL_FACT as
select * from DEID_VITAL_FACT_T;