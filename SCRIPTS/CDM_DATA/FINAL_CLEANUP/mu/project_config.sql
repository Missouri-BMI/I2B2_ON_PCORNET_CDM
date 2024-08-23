
-- project text
set table_name = {pm_schema} || '.' || 'pm_project_data';
set harvest_table = {source_schema} || '.' || 'HARVEST';
set project_text = 'NextGen Data Lake De-Identified';

update identifier($table_name)
set project_name =  $project_text || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from identifier($harvest_table)
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'ACT';

--QT_QUERY_RESULT_TYPE
set table_name = {target_schema} || '.' || 'QT_QUERY_RESULT_TYPE';
update identifier($table_name)
set VISUAL_ATTRIBUTE_TYPE_ID = 'LH'
where name = 'PATIENT_GPCSITE_COUNT_XML';


update identifier($table_name)
set classname = 'edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator'
where name = 'PATIENT_INOUT_XML';

--breakdown
set table_name = {target_schema} || '.' || 'QT_BREAKDOWN_PATH';

update identifier($table_name)
SET VALUE = '\\\\ACT_VISIT\\ACT\\Visit Details\\Visit type\\'
WHERE NAME = 'PATIENT_INOUT_XML';

--hive_cell_params
set table_name = {hive_schema} || '.' || 'hive_cell_params';

update identifier($table_name)
set value = '11'
where param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';
update identifier($table_name)
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.analysis.queue.large.maxjobcount';

update identifier($table_name)
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.analysis.queue.medium.maxjobcount';

update identifier($table_name)
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.lockout.setfinderquery.count';


update identifier($table_name)
set value = true
where PARAM_NAME_CD = 'queryprocessor.multifacttable';
