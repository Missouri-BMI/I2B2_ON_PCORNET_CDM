create or replace view {{ target_schema }}.VISIT_DIMENSION as
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        dim.ENCOUNTER_NUM as ENCOUNTER_NUM,
        dim.PATIENT_NUM as PATIENT_NUM,
    {% endif %}

    cast(null as VARCHAR(50)) as ACTIVE_STATUS_CD,

    {% if site == 'mu' %}
        TO_TIMESTAMP(admit_date :: DATE || ' ' || admit_time, 'YYYY-MM-DD HH24:MI:SS') as start_date,
        TO_TIMESTAMP(COALESCE(discharge_date, admit_date) :: DATE || ' ' || COALESCE(discharge_time, '00:00:00'), 'YYYY-MM-DD HH24:MI:SS') as end_date,
    {% else %}
        admit_date :: TIMESTAMP as start_date,
        discharge_date :: TIMESTAMP as end_date,
    {% endif %}

    ENC_TYPE as INOUT_CD,
    cast(null as VARCHAR(50)) as LOCATION_CD,
    cast(null as VARCHAR(900)) as LOCATION_PATH,
    datediff(day, start_date, end_date) as length_of_stay,
    cast(null as text) as VISIT_BLOB,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID,
    PAYER_TYPE_PRIMARY,
    facilityid,
    facility_location

from {{ source_schema }}.{{ encounter_table }} as dim
;