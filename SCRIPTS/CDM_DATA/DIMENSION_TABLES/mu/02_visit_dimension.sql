-- create view
create or replace view {target_schema}.VISIT_DIMENSION
as  
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM,
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM,
    cast(null as VARCHAR(50))                                                                       as ACTIVE_STATUS_CD,
    TO_TIMESTAMP(admit_date :: DATE || ' ' || admit_time, 'YYYY-MM-DD HH24:MI:SS')                  AS start_date,
    TO_TIMESTAMP(COALESCE(discharge_date, admit_date) :: DATE || ' ' || COALESCE(discharge_time, '00:00:00'), 'YYYY-MM-DD HH24:MI:SS')      AS end_date,
    ENC_TYPE                                                                                        as INOUT_CD,
    cast(null as VARCHAR(50))                                                                       as LOCATION_CD,
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
from {source_schema}.DEID_ENCOUNTER as dim;
