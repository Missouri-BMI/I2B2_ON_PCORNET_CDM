BEGIN
    USE SCHEMA i2b2hive;

    UPDATE crc_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE im_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE ont_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE work_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
END;

BEGIN
    USE SCHEMA I2B2PM;
    UPDATE pm_hive_data SET DOMAIN_NAME = 'nextgenbmi.umsystem.edu' WHERE DOMAIN_NAME = 'i2b2demo';
  
END;

UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/QueryToolService/' WHERE cell_id = 'CRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/FRService/' WHERE cell_id = 'FRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/OntologyService/' WHERE cell_id = 'ONT';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/WorkplaceService/' WHERE cell_id = 'WORK';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/IMService/' WHERE cell_id = 'IM';


insert into QT_QUERY_RESULT_TYPE(RESULT_TYPE_ID,NAME,DESCRIPTION,DISPLAY_TYPE_ID,VISUAL_ATTRIBUTE_TYPE_ID,CLASSNAME) values(5,'PATIENT_PCORNETSITE_COUNT_XML','PCORNet Site breakdown','CATNUM','LA','edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator')
;
INSERT INTO QT_BREAKDOWN_PATH(NAME,VALUE,CREATE_DATE) VALUES ('PATIENT_PCORNETSITE_COUNT_XML','\\\\ACT_DEMO\\ACT\\Demographics\\PCORNet Sites\\', CURRENT_TIMESTAMP);

INSERT INTO PM_PROJECT_DATA (PROJECT_ID, PROJECT_NAME, PROJECT_WIKI, PROJECT_PATH, STATUS_CD)
VALUES('ACT-PCORNET', 'PCORNet Deid datalake', 'http://www.i2b2.org', '/ACT-PCORNET', 'A');


INSERT INTO CRC_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', '/ACT-PCORNET/', '@', 'I2B2DATA', 'java:/QueryTool-PCORNET', 'SNOWFLAKE', 'Demo', NULL, NULL, NULL, NULL, NULL);


INSERT INTO ONT_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-PCORNET/', '@', 'I2B2METADATA', 'java:/Ontology-PCORNET', 'SNOWFLAKE', 'Metadata', NULL, NULL, NULL, NULL, NULL);
  
INSERT INTO WORK_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-PCORNET/', '@', 'I2B2WORKDATA', 'java:/Workplace-PCORNET', 'SNOWFLAKE', 'Workplace', NULL, NULL, NULL, NULL, NULL);
  