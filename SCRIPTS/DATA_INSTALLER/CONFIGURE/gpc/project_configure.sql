USE SCHEMA {crc_schema};

INSERT INTO QT_BREAKDOWN_PATH(NAME,VALUE,CREATE_DATE)
VALUES ('PATIENT_GPCSITE_COUNT_XML','\\\\ACT_DEMO\\ACT\\Demographics\\GPC Sites\\', CURRENT_TIMESTAMP)
;

insert into QT_QUERY_RESULT_TYPE(RESULT_TYPE_ID,NAME,DESCRIPTION,DISPLAY_TYPE_ID,VISUAL_ATTRIBUTE_TYPE_ID,CLASSNAME) 
values(21, 'PATIENT_GPCSITE_COUNT_XML','GPC Site breakdown','CATNUM','LA','edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator')
;

//TODO:
use schema {project_pm};
INSERT INTO PM_PROJECT_DATA (PROJECT_ID, PROJECT_NAME, PROJECT_WIKI, PROJECT_PATH, STATUS_CD)
VALUES('ACT-GPC', 'GPC Deid datalake', 'http://www.i2b2.org', '/ACT-GPC', 'A');
UPDATE pm_hive_data SET DOMAIN_NAME = 'nextgenbmi.umsystem.edu' WHERE DOMAIN_NAME = 'i2b2demo';

use schema {project_hive};

UPDATE crc_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE im_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE ont_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE work_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';

INSERT INTO CRC_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', '/ACT-GPC/', '@', 'I2B2DATA', 'java:/QueryTool-GPC', 'SNOWFLAKE', 'Demo', NULL, NULL, NULL, NULL, NULL);

INSERT INTO ONT_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-GPC/', '@', 'I2B2METADATA', 'java:/Ontology-GPC', 'SNOWFLAKE', 'Metadata', NULL, NULL, NULL, NULL, NULL);
  
INSERT INTO WORK_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-GPC/', '@', 'I2B2WORKDATA', 'java:/Workplace-GPC', 'SNOWFLAKE', 'Workplace', NULL, NULL, NULL, NULL, NULL);
  