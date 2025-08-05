create or replace view {{ target_schema }}.LAB_FACT as
-- Numeric results
select
    {%- if project == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {%- elif project == 'washu' %}
        ec.ENCOUNTER_NUM as ENCOUNTER_NUM,
        pc.PATIENT_NUM as PATIENT_NUM,
    {%- else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {%- endif %}
    concat('LOINC:', LAB_LOINC) as CONCEPT_CD,
    '@' as PROVIDER_ID,
    LAB_ORDER_DATE :: TIMESTAMP as START_DATE,
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('N' as VARCHAR(50)) as VALTYPE_CD,
    CASE
        when result_modifier = 'LT' then 'L'
        when result_modifier = 'EQ' then 'E'
        when result_modifier = 'GT' then 'G'
        else 'E'
    END as TVAL_CHAR,
    cast(RESULT_NUM as DECIMAL(18, 5)) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast(RESULT_UNIT as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from
    {%- if project == 'mu' %}
        {{ source_schema }}.DEID_LAB_RESULT_CM fact
    {%- elif project == 'washu' %}
        {{ source_schema }}.V_DEID_LAB_RESULT_CM fact
        left join CDM_DATALAKE.GPC.ENCOUNTER_CROSSWALK as ec
            on fact.encounterid = ec.encounterid and ec.pcornet_site_name = 'C4WU'
        left join CDM_DATALAKE.GPC.PATIENT_CROSSWALK as pc
            on fact.patid = pc.patid and pc.pcornet_site_name = 'C4WU'
    {%- elif project == 'gpc' %}
        {{ source_schema }}.GPC_DEID_LAB_RESULT_CM fact
    {%- elif project == 'pcornet' %}
        {{ source_schema }}.PCORNET_DEID_LAB_RESULT_CM fact
    {%- endif %}
where LAB_LOINC is not null and result_modifier <> 'TX'
union all
-- Qualitative results (text)
select
    {%- if project == 'mu' %}
        cast(ENCOUNTERID as NUMBER(38, 0)) as ENCOUNTER_NUM,
        cast(PATID as NUMBER(38, 0)) as PATIENT_NUM,
    {%- elif project == 'washu' %}
        ec.ENCOUNTER_NUM as ENCOUNTER_NUM,
        pc.PATIENT_NUM as PATIENT_NUM,
    {%- else %}
        fact.ENCOUNTER_NUM as ENCOUNTER_NUM,
        fact.PATIENT_NUM as PATIENT_NUM,
    {%- endif %}
    concat('LOINC:', LAB_LOINC) as CONCEPT_CD,
    '@' as PROVIDER_ID,
    LAB_ORDER_DATE :: TIMESTAMP as START_DATE,
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    cast('T' as VARCHAR(50)) as VALTYPE_CD,
    cast(RESULT_QUAL as VARCHAR(255)) as TVAL_CHAR,
    cast(null as DECIMAL(18, 5)) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    cast(RESULT_UNIT as VARCHAR(50)) as UNITS_CD,
    cast(null as TIMESTAMP) as END_DATE,
    '@' as LOCATION_CD,
    cast(null as text) as OBSERVATION_BLOB,
    cast(null as integer) as CONFIDENCE_NUM,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    CURRENT_TIMESTAMP as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP as IMPORT_DATE,
    cast(null as VARCHAR(50)) as SOURCESYSTEM_CD,
    cast(null as integer) as UPLOAD_ID
from
    {%- if project == 'mu' %}
        {{ source_schema }}.DEID_LAB_RESULT_CM fact
    {%- elif project == 'washu' %}
        {{ source_schema }}.V_DEID_LAB_RESULT_CM fact
        left join CDM_DATALAKE.GPC.ENCOUNTER_CROSSWALK as ec
            on fact.encounterid = ec.encounterid and ec.pcornet_site_name = 'C4WU'
        left join CDM_DATALAKE.GPC.PATIENT_CROSSWALK as pc
            on fact.patid = pc.patid and pc.pcornet_site_name = 'C4WU'
    {%- elif project == 'gpc' %}
        {{ source_schema }}.GPC_DEID_LAB_RESULT_CM fact
    {%- elif project == 'pcornet' %}
        {{ source_schema }}.PCORNET_DEID_LAB_RESULT_CM fact
    {%- endif %}
where LAB_LOINC is not null and result_modifier = 'TX'
;
