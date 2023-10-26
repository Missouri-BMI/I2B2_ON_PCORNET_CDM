/* rename project */
set update_date = '#update_date';
set project_text = '#project_text';

set display_text = $project_text || ' (' || $update_date || ')';

set target_pm_schema = $pm_db || '.' || 'i2b2pm';
set target_hive_schema = $pm_db || '.' || 'I2B2HIVE';

set table_name = $target_pm_schema || '.' || 'pm_project_data';
update identifier($table_name)
set project_name = $display_text
where project_id = 'ACT';

set table_name = $target_hive_schema || '.' || 'hive_cell_params';
update identifier($table_name)
set value = '11'
where param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';

--gpc hide
set table_name = $target_schema || '.' || 'QT_QUERY_RESULT_TYPE';
update identifier($table_name)
set VISUAL_ATTRIBUTE_TYPE_ID = 'LH'
where name = 'PATIENT_GPCSITE_COUNT_XML';

--visit details
set table_name = $target_schema || '.' || 'QT_QUERY_RESULT_TYPE';
update identifier($table_name)
set classname = 'edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator'
where name = 'PATIENT_INOUT_XML';

set table_name = $target_schema || '.' || 'QT_BREAKDOWN_PATH';
update identifier($table_name)
SET VALUE = '\\\\ACT_VISIT\\ACT\\Visit Details\\Visit type\\'
WHERE NAME = 'PATIENT_INOUT_XML';

