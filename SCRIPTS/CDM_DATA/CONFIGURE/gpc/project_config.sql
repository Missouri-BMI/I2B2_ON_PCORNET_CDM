update {project_pm}.pm_project_data
set project_name =  'GPC Data Lake' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'SANDBOX-GPC';
