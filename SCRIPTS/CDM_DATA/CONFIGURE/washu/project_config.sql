update {project_pm}.pm_project_data
set project_name =  'WashU De-Identified' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from {source_schema}.V_DEID_HARVEST
    where datamartid = 'C4WU'
) || ')'
where project_id = 'ACT';