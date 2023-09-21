use database I2B2_SANDBOX_VASANTHI;
use schema I2B2DATA;

create or replace view PRIVATE_VISIT_DIMENSION
as 
select 
    dim.*,
    pc.i2b2_patid                                as I2B2_PATIENT_NUM,
    ec.i2b2_encounterid                          as I2B2_ENCOUNTER_NUM,
    cast(null as VARCHAR(50))                    as I2B2_ACTIVE_STATUS_CD,
    cast(null as VARCHAR(50))                    as I2B2_LOCATION_CD,
    cast(null as VARCHAR(900))                   as I2B2_LOCATION_PATH,
    cast(null as text)                           as I2B2_VISIT_BLOB,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_UPDATE_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_DOWNLOAD_DATE,
    cast(CURRENT_DATE()  as TIMESTAMP)           as I2B2_IMPORT_DATE,
    cast('CDM 6.0' 	as VARCHAR(50))          as I2B2_SOURCESYSTEM_CD,    
    cast(null	as integer)                      as I2B2_UPLOAD_ID
from DEIDENTIFIED_PCORNET_CDM.CDM_2023_JULY.DEID_ENCOUNTER as dim
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.PATIENT_CROSSWALK as pc
using (patid)
inner join I2B2_PCORNET_CDM.CDM_2023_JULY.ENCOUNTER_CROSSWALK as ec
using (ENCOUNTERID); 


-- create view
create or replace view VISIT_DIMENSION
as  
select
    I2B2_ENCOUNTER_NUM as encounter_num,
    I2B2_PATIENT_NUM   as patient_num,
    COALESCE(providerID, '-1') as provider_id,
    admit_date as start_date,
    discharge_date as end_date,
    ENC_TYPE as INOUT_CD,
    facilityid,
    facility_location,
    datediff(day, admit_date, discharge_date) as length_of_stay,
    I2B2_ACTIVE_STATUS_CD as ACTIVE_STATUS_CD,
    PAYER_TYPE_PRIMARY,
    I2B2_LOCATION_CD as LOCATION_CD,
    I2B2_LOCATION_PATH as LOCATION_PATH,
    I2B2_VISIT_BLOB as VISIT_BLOB,
    I2B2_UPDATE_DATE as UPDATE_DATE,
    I2B2_DOWNLOAD_DATE as DOWNLOAD_DATE,
    I2B2_IMPORT_DATE as IMPORT_DATE,
    I2B2_SOURCESYSTEM_CD as SOURCESYSTEM_CD,
    I2B2_UPLOAD_ID as UPLOAD_ID
from PRIVATE_VISIT_DIMENSION as dim;       

