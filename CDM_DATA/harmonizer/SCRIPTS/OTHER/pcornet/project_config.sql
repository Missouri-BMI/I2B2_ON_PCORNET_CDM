set source_schema = '#source_schema';
set target_db = '#target_db';
set target_schema = $target_db || '.' || '#target_schema';
set pm_db = '#pm_db';

/* rename project */
set project_text = 'PCORNet Deid datalake';
set harvest = $source_schema || '.' || 'PCORNET_DEID_HARVEST';

set target_pm_schema = $pm_db || '.' || 'i2b2pm';
set target_hive_schema = $pm_db || '.' || 'I2B2HIVE';

set table_name = $target_pm_schema || '.' || 'pm_project_data';
update identifier($table_name)
set project_name =  $project_text || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from identifier($harvest)
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'ACT-PCORNET';

set table_name = $target_hive_schema || '.' || 'hive_cell_params';
update identifier($table_name)
set value = '11'
where param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';


--pcornet breakdown
set table_name = $target_schema || '.' || 'QT_QUERY_RESULT_TYPE';
update identifier($table_name)
set VISUAL_ATTRIBUTE_TYPE_ID = 'LA'
where name = 'PATIENT_PCORNETSITE_COUNT_XML';

--visit details
set table_name = $target_schema || '.' || 'QT_QUERY_RESULT_TYPE';
update identifier($table_name)
set classname = 'edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator'
where name = 'PATIENT_INOUT_XML';

set table_name = $target_schema || '.' || 'QT_BREAKDOWN_PATH';
update identifier($table_name)
SET VALUE = '\\\\ACT_VISIT\\ACT\\Visit Details\\Visit type\\'
WHERE NAME = 'PATIENT_INOUT_XML';


set table_name = $target_hive_schema || '.' || 'hive_cell_params';
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