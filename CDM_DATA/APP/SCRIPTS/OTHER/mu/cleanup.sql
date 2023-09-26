/* rename project */

set update_date = 'July 2023';
set project = 'NextGen Data Lake De-Identified';
set display_text = $project || ' (' || $update_date || ')';


update i2b2pm.pm_project_data
set project_name = $display_text
where project_id = 'ACT';


update I2B2HIVE.HIVE_CELL_PARAMS
set value = '11'
where param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';