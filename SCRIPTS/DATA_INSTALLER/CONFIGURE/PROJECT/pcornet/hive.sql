use schema {project_hive};


UPDATE crc_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE im_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE ont_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE work_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';


INSERT INTO CRC_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', '/ACT-PCORNET/', '@', 'I2B2DATA', 'java:/QueryTool-PCORNET', 'SNOWFLAKE', 'Demo', NULL, NULL, NULL, NULL, NULL);

INSERT INTO ONT_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-PCORNET/', '@', 'I2B2METADATA', 'java:/Ontology-PCORNET', 'SNOWFLAKE', 'Metadata', NULL, NULL, NULL, NULL, NULL);
  
INSERT INTO WORK_DB_LOOKUP(c_domain_id, c_project_path, c_owner_id, c_db_fullschema, c_db_datasource, c_db_servertype, c_db_nicename, c_db_tooltip, c_comment, c_entry_date, c_change_date, c_status_cd)
  VALUES('nextgenbmi.umsystem.edu', 'ACT-PCORNET/', '@', 'I2B2WORKDATA', 'java:/Workplace-PCORNET', 'SNOWFLAKE', 'Workplace', NULL, NULL, NULL, NULL, NULL);
  