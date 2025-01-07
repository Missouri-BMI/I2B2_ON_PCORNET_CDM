USE SCHEMA {project_hive};

UPDATE crc_db_lookup SET C_DOMAIN_ID = 'wustl.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE im_db_lookup SET C_DOMAIN_ID = 'wustl.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE ont_db_lookup SET C_DOMAIN_ID = 'wustl.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE work_db_lookup SET C_DOMAIN_ID = 'wustl.edu' WHERE C_DOMAIN_ID = 'i2b2demo';

USE SCHEMA {project_pm};

UPDATE pm_hive_data SET DOMAIN_NAME = 'wustl.edu' WHERE DOMAIN_NAME = 'i2b2demo';