set search_path to i2b2hive;

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

truncate crc_db_lookup;
INSERT INTO CRC_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', '/ACT/', '@', 'I2B2DATA', 'java:/QueryToolDemoDS', 'SNOWFLAKE', 'Demo', NULL, NULL, NULL, NULL, NULL);

truncate ont_db_lookup;
INSERT INTO ONT_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT/', '@', 'I2B2METADATA', 'java:/OntologyDemoDS', 'SNOWFLAKE', 'Metadata', NULL, NULL, NULL, NULL, NULL);

truncate work_db_lookup;
INSERT INTO WORK_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT/', '@', 'i2b2workdata', 'java:/WorkplaceDemoDS', 'POSTGRESQL', 'Workplace', NULL, NULL, NULL, NULL, NULL);
  


set search_path to i2b2pm;

UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/QueryToolService/' WHERE cell_id = 'CRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/FRService/' WHERE cell_id = 'FRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/OntologyService/' WHERE cell_id = 'ONT';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/WorkplaceService/' WHERE cell_id = 'WORK';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/IMService/' WHERE cell_id = 'IM';

--
truncate pm_user_data; 

-- rotating passwords in run time
INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('AGG_SERVICE_ACCOUNT', 'AGG_SERVICE_ACCOUNT', '9117d59a69dc49807671a51f10ab7f', 'A');

-- rotating passwords in run time
INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('mhmcb@umsystem.edu', 'Md Saber Hossain', '9117d59a69dc49807671a51f10ab7f', 'A');

--
truncate PM_PROJECT_USER_ROLES;

INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('@', 'mhmcb@umsystem.edu', 'ADMIN', 'A');

-- 
truncate PM_USER_PARAMS;
INSERT INTO PM_USER_PARAMS (DATATYPE_CD, USER_ID, PARAM_NAME_CD, VALUE, CHANGE_DATE, ENTRY_DATE, STATUS_CD)
values('T', 'mhmcb@umsystem.edu', 'authentication_method', 'SAML', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'A');
UPDATE pm_hive_data SET DOMAIN_NAME = 'nextgenbmi.umsystem.edu' WHERE DOMAIN_NAME = 'i2b2demo';

--
DELETE FROM pm_project_user_roles where project_id = 'ACT';

INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'USER', 'A');

INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'EDITOR', 'A');

INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_DEID', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_LDS', 'A');

INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_PROT', 'A');


set search_path to i2b2workdata;
truncate WORKPLACE_ACCESS;

INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
  VALUES('act', 'WORKPLACE','N', 0, 'SHARED', 'shared', 'ACT', 'Y', 100, NULL, 'CA', 'SHARED');

INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
 VALUES('act', 'WORKPLACE','N', 0, '@', '@', '@', 'N', 0, NULL, 'CA', '@');
