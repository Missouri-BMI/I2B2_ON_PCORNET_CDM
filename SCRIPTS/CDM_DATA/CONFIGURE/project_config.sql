{% if site == 'mu' %}
update pm_project_data
set project_name =  'NextGen Data Lake De-Identified' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'ACT';

{% elif site == 'washu' %}
update pm_project_data
set project_name =  'WashU De-Identified' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.V_DEID_HARVEST
    where datamartid = 'C4WU'
) || ')'
where project_id = 'ACT';

{% elif site == 'gpc' %}

update pm_project_data
set project_name =  'GPC Data Lake' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.GPC_DEID_HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'SANDBOX-GPC';


{% elif site == 'pcornet' %}

update pm_project_data
set project_name =  'PCORNet Deid datalake' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.PCORNET_DEID_HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'ACT-PCORNET';

{% endif %}







