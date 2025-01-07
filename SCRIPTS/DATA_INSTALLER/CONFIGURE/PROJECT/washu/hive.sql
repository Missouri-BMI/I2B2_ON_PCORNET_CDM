USE SCHEMA {project_hive};

UPDATE crc_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE im_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE ont_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
UPDATE work_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';