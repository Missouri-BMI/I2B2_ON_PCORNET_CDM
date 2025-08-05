create or replace view {{ target_schema }}.DIAGNOSIS_FACT as
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

    case
        when dx_type = '10' then concat('ICD10CM:', dx)
        when dx_type = '09' then 
            case
                when right(dx, 1) = '.' then concat('ICD9CM:', split_part(dx, '.', 1))
                else concat('ICD9CM:', dx)
            end
        else concat(dx_type, ':', dx)
    end as CONCEPT_CD,
    COALESCE(fact.PROVIDERID, '@') as PROVIDER_ID,
    ADMIT_DATE :: TIMESTAMP as START_DATE,
    '@' as MODIFIER_CD,
    1 as INSTANCE_NUM,
    '' as VALTYPE_CD,
    '' as TVAL_CHAR,
    cast(null as integer) as NVAL_NUM,
    '' as VALUEFLAG_CD,
    cast(null as integer) as QUANTITY_NUM,
    '@' as UNITS_CD,
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
        {{ source_schema }}.DEID_DIAGNOSIS fact
    {%- elif project == 'washu' %}
        {{ source_schema }}.V_DEID_DIAGNOSIS fact
        left join CDM_DATALAKE.GPC.ENCOUNTER_CROSSWALK as ec
            on fact.encounterid = ec.encounterid and ec.pcornet_site_name = 'C4WU'
        left join CDM_DATALAKE.GPC.PATIENT_CROSSWALK as pc
            on fact.patid = pc.patid and pc.pcornet_site_name = 'C4WU'
    {%- elif project == 'gpc' %}
        {{ source_schema }}.GPC_DEID_DIAGNOSIS fact
    {%- elif project == 'pcornet' %}
        {{ source_schema }}.PCORNET_DEID_DIAGNOSIS fact
    {%- endif %}
;
