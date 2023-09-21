use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace view PRIVATE_PROVIDER_DIMENSION
as 
select 
    dim.* ,
    cast('@'  as VARCHAR(700))                   as I2B2_PROVIDER_PATH,
    cast(null as VARCHAR(850))                   as I2B2_NAME_CHAR,
    cast(null as text)                           as I2B2_PROVIDER_BLOB,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_UPDATE_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_DOWNLOAD_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_IMPORT_DATE,
    cast($cdm_version 	as VARCHAR(50))          as I2B2_SOURCESYSTEM_CD,         
    cast(null	as INT)                          as I2B2_UPLOAD_ID
from DEIDENTIFIED_PCORNET_CDM.CDM_2023_JULY.DEID_PROVIDER as dim;            


-- create view
create or replace view DEID_PROVIDER_DIMENSION
as 
select 
    PROVIDERID as PROVIDER_ID,
    PROVIDER_SEX,
    I2B2_PROVIDER_PATH as PROVIDER_PATH,
    I2B2_NAME_CHAR as NAME_CHAR,
    I2B2_PROVIDER_BLOB as PROVIDER_BLOB,
    PROVIDER_SPECIALTY_PRIMARY,
    PROVIDER_NPI,
    PROVIDER_NPI_FLAG,
    RAW_PROVIDER_SPECIALTY_PRIMARY,
    I2B2_UPDATE_DATE as UPDATE_DATE,
    I2B2_DOWNLOAD_DATE as DOWNLOAD_DATE,
    I2B2_IMPORT_DATE as IMPORT_DATE,
    I2B2_SOURCESYSTEM_CD as SOURCESYSTEM_CD,
    I2B2_UPLOAD_ID as UPLOAD_ID
from PRIVATE_PROVIDER_DIMENSION;