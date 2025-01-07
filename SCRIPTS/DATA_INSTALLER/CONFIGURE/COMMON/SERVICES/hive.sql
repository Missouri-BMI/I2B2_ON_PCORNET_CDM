USE SCHEMA {hive_schema};

update hive_cell_params
set value = '11'
where param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';
update hive_cell_params
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.analysis.queue.large.maxjobcount';

update hive_cell_params
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.analysis.queue.medium.maxjobcount';

update hive_cell_params
set value = 1000
where PARAM_NAME_CD = 'edu.harvard.i2b2.crc.lockout.setfinderquery.count';


update hive_cell_params
set value = true
where PARAM_NAME_CD = 'queryprocessor.multifacttable';
