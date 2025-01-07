-- create view
create or replace view {target_schema}.VISIT_DIMENSION
as  
select
    ec.ENCOUNTER_NUM                                                                                as ENCOUNTER_NUM, 
    pc.PATIENT_NUM                                                                                  as PATIENT_NUM,
    cast(null as VARCHAR(50))                                                                       as ACTIVE_STATUS_CD,
    -- TO_TIMESTAMP(admit_date :: DATE || ' ' || admit_time, 'YYYY-MM-DD HH24:MI:SS')                  AS start_date,
    -- TO_TIMESTAMP(COALESCE(discharge_date, admit_date) :: DATE || ' ' || COALESCE(discharge_time, '00:00:00'), 'YYYY-MM-DD HH24:MI:SS')      AS end_date,
    admit_date :: TIMESTAMP                                                                         AS start_date,
    discharge_date :: TIMESTAMP                                                                     AS end_date,
    ENC_TYPE                                                                                        as INOUT_CD,
    cast(null as VARCHAR(50))                                                                      as LOCATION_CD,
    cast(null as VARCHAR(900))                                                                      as LOCATION_PATH,
    datediff(day, start_date, end_date)                                                             as length_of_stay,
    cast(null as text)                                                                              as VISIT_BLOB,
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,
    cast(null as  integer)                                                                          as UPLOAD_ID,
    PAYER_TYPE_PRIMARY,
    facilityid,
    facility_location
from {source_schema}.GPC_DEID_ENCOUNTER as dim   
inner join {target_schema}.patient_crosswalk as pc
using (patid)
inner join {target_schema}.encounter_crosswalk as ec
using (ENCOUNTERID);