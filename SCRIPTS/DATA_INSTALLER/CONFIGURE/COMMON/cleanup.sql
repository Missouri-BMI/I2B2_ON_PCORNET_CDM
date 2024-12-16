USE SCHEMA {crc_schema};
DROP TABLE IF EXISTS patient_dimension;
DROP TABLE IF EXISTS visit_dimension;
DROP TABLE IF EXISTS provider_dimension;


update QT_QUERY_RESULT_TYPE
set classname = 'edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator'
where name = 'PATIENT_INOUT_XML';

update QT_BREAKDOWN_PATH
SET VALUE = '\\\\ACT_VISIT\\ACT\\Visit Details\\Visit type\\'
WHERE NAME = 'PATIENT_INOUT_XML';

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


USE SCHEMA {pm_schema};

UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/QueryToolService/' WHERE cell_id = 'CRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/FRService/' WHERE cell_id = 'FRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/OntologyService/' WHERE cell_id = 'ONT';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/WorkplaceService/' WHERE cell_id = 'WORK';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/IMService/' WHERE cell_id = 'IM';


USE SCHEMA {wd_schema};

INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
  VALUES('act', 'WORKPLACE','N', 0, 'SHARED', 'shared', 'ACT', 'Y', 100, NULL, 'CA', 'SHARED');

INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
 VALUES('act', 'WORKPLACE','N', 0, '@', '@', '@', 'N', 0, NULL, 'CA', '@');
