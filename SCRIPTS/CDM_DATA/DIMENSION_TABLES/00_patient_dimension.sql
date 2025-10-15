create or replace view {{ target_schema }}.PATIENT_DIMENSION as
select
    {%- if site == 'mu' -%}
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {%- else -%}
        dim.PATIENT_NUM as PATIENT_NUM,
    {%- endif %}
    CASE WHEN dead.DEATH_DATE is not null THEN 'Y' ELSE 'N' END as VITAL_STATUS_CD,
    dim.birth_date :: TIMESTAMP as BIRTH_DATE,
    cast(null as TIMESTAMP) as DEATH_DATE,
    COALESCE(dim.sex, 'NI') as SEX_CD,
    PAT_PREF_LANGUAGE_SPOKEN as LANGUAGE_CD,
    COALESCE(dim.hispanic, 'NI') as HISPANIC,
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
        WHEN dim.RACE is null THEN 'No information'
        ELSE 'No information'
    END as RACE_CD,

    cast(null as VARCHAR(50)) as MARITAL_STATUS_CD,
    cast(null as integer)   as AGE_IN_YEARS_NUM,
    cast(null as VARCHAR(50)) as RELIGION_CD,
    cast(null as VARCHAR(10)) as ZIP_CD,
    cast(null as VARCHAR(700)) as STATECITYZIP_PATH,
    cast(null as VARCHAR(50)) as INCOME_CD,
    cast(null as text) as PATIENT_BLOB,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as INT) as UPLOAD_ID
    {%- if site == 'gpc'%}
        , PCORNET_SITE_ID as SITE_ID
        , PCORNET_SITE_NAME as SITE_NAME
    {%- endif %}
from {{ source_schema }}.{{ demographic_table }} as dim
left join {{ source_schema }}.{{ death_table }} as dead
using (patient_num)
;
