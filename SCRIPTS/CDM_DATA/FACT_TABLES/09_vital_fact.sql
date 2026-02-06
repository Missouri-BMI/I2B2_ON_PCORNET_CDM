create or replace view {{ target_schema }}.VITAL_FACT as

-- Height (HT)
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {% endif %}
    'LOINC:8302-2' as CONCEPT_CD,
    '@' as PROVIDER_ID,
    {% if site == 'mu' %}
        TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_DATE,
    {% else %}
        MEASURE_DATE :: TIMESTAMP as START_DATE,
    {% endif %}
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    'E' as TVAL_CHAR,
    COALESCE(cast(fact.HT as DECIMAL(18, 5)), null) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast('inches' as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from {{ source_schema }}.{{ vital_table }} fact
where
    {% if site == 'mu' %}
        ENCOUNTERID is not null
    {% else %}
        fact.ENCOUNTER_NUM is not null
    {% endif %}

union all

-- Weight (WT)
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {% endif %}
    'LOINC:3141-9' as CONCEPT_CD,
    '@' as PROVIDER_ID,
    {% if site == 'mu' %}
        TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_DATE,
    {% else %}
        MEASURE_DATE :: TIMESTAMP as START_DATE,
    {% endif %}
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    'E' as TVAL_CHAR,
    COALESCE(cast(fact.WT as DECIMAL(18, 5)), null) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast('kg' as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from {{ source_schema }}.{{ vital_table }} fact
where
    {% if site == 'mu' %}
        ENCOUNTERID is not null
    {% else %}
        fact.ENCOUNTER_NUM is not null
    {% endif %}

union all

-- Diastolic BP
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {% endif %}
    'LOINC:8462-4' as CONCEPT_CD,
    '@' as PROVIDER_ID,
    {% if site == 'mu' %}
        TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_DATE,
    {% else %}
        MEASURE_DATE :: TIMESTAMP as START_DATE,
    {% endif %}
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    'E' as TVAL_CHAR,
    COALESCE(cast(fact.DIASTOLIC as DECIMAL(18, 5)), null) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast('mm Hg' as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from {{ source_schema }}.{{ vital_table }} fact
where
    {% if site == 'mu' %}
        ENCOUNTERID is not null
    {% else %}
        fact.ENCOUNTER_NUM is not null
    {% endif %}

union all

-- Systolic BP
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {% endif %}
    'LOINC:8480-6' as CONCEPT_CD,
    '@' as PROVIDER_ID,
    {% if site == 'mu' %}
        TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_DATE,
    {% else %}
        MEASURE_DATE :: TIMESTAMP as START_DATE,
    {% endif %}
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    'E' as TVAL_CHAR,
    COALESCE(cast(fact.SYSTOLIC as DECIMAL(18, 5)), null) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast('mm Hg' as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from  {{ source_schema }}.{{ vital_table }} fact
where
    {% if site == 'mu' %}
        ENCOUNTERID is not null
    {% else %}
        fact.ENCOUNTER_NUM is not null
    {% endif %}

union all

-- BMI
select
    {% if site == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {% else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {% endif %}
    'LOINC:39156-5' as CONCEPT_CD,
    '@' as PROVIDER_ID,
    {% if site == 'mu' %}
        TO_TIMESTAMP(MEASURE_DATE :: DATE || ' ' || MEASURE_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_DATE,
    {% else %}
        MEASURE_DATE :: TIMESTAMP as START_DATE,
    {% endif %}
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    'E' as TVAL_CHAR,
    COALESCE(cast(fact.ORIGINAL_BMI as DECIMAL(18, 5)), null) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast('kg/m2' as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from {{ source_schema }}.{{ vital_table }} fact
where
    {% if site == 'mu' %}
        ENCOUNTERID is not null
    {% else %}
        fact.ENCOUNTER_NUM is not null
    {% endif %}
;
