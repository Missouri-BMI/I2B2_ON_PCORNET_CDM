use schema {{ pm_schema }};
{% if site == 'mu' %}
UPDATE pm_project_data
SET project_name = 'NextGen Data Lake De-Identified' || ' (' || (
    SELECT TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY')
    FROM {{ source_schema }}.HARVEST
    WHERE datamartid = 'C4UMO'
) || ')'
WHERE project_id = 'ACT';

{% elif site == 'washu' %}
UPDATE pm_project_data
SET project_name = 'WashU De-Identified' || ' (' || (
    SELECT TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY')
    FROM {{ source_schema }}.V_DEID_HARVEST
    WHERE datamartid = 'C4WU'
) || ')'
WHERE project_id = 'ACT';

{% elif site == 'gpc' %}
UPDATE pm_project_data
SET project_name = 'GPC Data Lake' || ' (' || (
    SELECT TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY')
    FROM {{ source_schema }}.GPC_DEID_HARVEST
    WHERE datamartid = 'C4UMO'
) || ')'
WHERE project_id = 'SANDBOX-GPC';

{% elif site == 'pcornet' %}
UPDATE pm_project_data
SET project_name = 'PCORNet Deid datalake' || ' (' || (
    SELECT TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY')
    FROM {{ source_schema }}.PCORNET_DEID_HARVEST
    WHERE datamartid = 'C4UMO'
) || ')'
WHERE project_id = 'ACT-PCORNET';

{% endif %}