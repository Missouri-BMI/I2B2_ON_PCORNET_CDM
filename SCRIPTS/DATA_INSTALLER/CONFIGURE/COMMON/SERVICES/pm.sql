USE SCHEMA {pm_schema};

UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/QueryToolService/' WHERE cell_id = 'CRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/FRService/' WHERE cell_id = 'FRC';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/OntologyService/' WHERE cell_id = 'ONT';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/WorkplaceService/' WHERE cell_id = 'WORK';
UPDATE pm_cell_data SET url = 'http://127.0.0.1/i2b2/services/IMService/' WHERE cell_id = 'IM';

--
truncate pm_user_data; 

INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('AGG_SERVICE_ACCOUNT', 'AGG_SERVICE_ACCOUNT', '9117d59a69dc49807671a51f10ab7f', 'A');

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
