create or replace table PRIVATE_PATIENT_DIMENSION
as 
select 
    dim.* ,
    COALESCE(dim.hispanic, 'NI')                 as I2B2_HISPANIC,
    COALESCE(dim.sex, 'NI')                      as I2B2_SEX_CD,
    CASE
        WHEN dim.RACE = '01' THEN 'American Indian or Alaska Native' 
        WHEN dim.RACE = '02' THEN 'Asian'
        WHEN dim.RACE = '03' THEN 'Black or African American'
        WHEN dim.RACE = '04' THEN 'Native Hawaiian or Other Pacific Islander'
        WHEN dim.RACE = '05' THEN 'White'
        WHEN dim.RACE = '06' THEN 'Multiple race'
        WHEN dim.RACE = '07' THEN 'Refuse to answer'
        WHEN dim.RACE = 'NI' THEN 'No information'
        WHEN dim.RACE = 'UN' THEN 'Unknown'
        WHEN dim.RACE = 'OT' THEN 'Other'
        WHEN dim.RACE is null then 'No information'
        ELSE 'No information'
    END AS I2B2_RACE_CD,
    CASE
        WHEN dead.DEATH_DATE is not null THEN 'Y'
        ELSE 'N'
    END                                          as I2B2_VITAL_STATUS_CD,
    pc.i2b2_patid                                as I2B2_PATIENT_NUM,                                    
    cast(null  as TIMESTAMP)                     as I2B2_DEATH_DATE,
    cast(null as integer)                        as I2B2_AGE_IN_YEARS_NUM,
    cast(null as VARCHAR(50))                    as I2B2_RELIGION_CD,
    cast(null as VARCHAR(10))                    as I2B2_ZIP_CD,
    cast(null as VARCHAR(700))                   as I2B2_STATECITYZIP_PATH,
    cast(null as VARCHAR(50))                    as I2B2_INCOME_CD,
    cast(null as text)                           as I2B2_PATIENT_BLOB,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_UPDATE_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_DOWNLOAD_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_IMPORT_DATE,
    cast($cdm_version 	as VARCHAR(50))      as I2B2_SOURCESYSTEM_CD,        
    cast(null	as INT)                      as I2B2_UPLOAD_ID
from identifier($patient_source_table) as dim
inner join identifier($patient_crosswalk) as pc
using(patid)
left join identifier($source_death) as dead  
using (patid);

-- create view
create or replace view "DEID_PATIENT_DIMENSION" as 
select 
    I2B2_PATIENT_NUM as PATIENT_NUM,
    I2B2_VITAL_STATUS_CD as VITAL_STATUS_CD,
    birth_date as BIRTH_DATE,
    I2B2_DEATH_DATE as DEATH_DATE,
    I2B2_SEX_CD as SEX_CD,
    PAT_PREF_LANGUAGE_SPOKEN as language_cd,
    I2B2_HISPANIC as hispanic,
    I2B2_RACE_CD as race_cd,
    --warning remove if not gpc
    -- GPC_SITE,
    I2B2_AGE_IN_YEARS_NUM as AGE_IN_YEARS_NUM,
    I2B2_RELIGION_CD as RELIGION_CD,
    I2B2_ZIP_CD as ZIP_CD,
    I2B2_STATECITYZIP_PATH as STATECITYZIP_PATH,
    I2B2_INCOME_CD as INCOME_CD,
    I2B2_PATIENT_BLOB as PATIENT_BLOB,
    I2B2_UPDATE_DATE as UPDATE_DATE,
    I2B2_DOWNLOAD_DATE as DOWNLOAD_DATE,
    I2B2_IMPORT_DATE as IMPORT_DATE,
    I2B2_SOURCESYSTEM_CD as SOURCESYSTEM_CD,
    I2B2_UPLOAD_ID as UPLOAD_ID
from "PRIVATE_PATIENT_DIMENSION";