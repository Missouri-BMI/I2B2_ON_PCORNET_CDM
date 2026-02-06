
set search_path to i2b2pm;


CREATE TABLE PM_CELL_DATA ( 
	CELL_ID     	VARCHAR(50) NOT NULL,
	PROJECT_PATH	VARCHAR(255) NOT NULL,
	NAME        	VARCHAR(255) NULL,
	METHOD_CD      	VARCHAR(255) NULL,
	URL         	VARCHAR(255) NULL,
	CAN_OVERRIDE	INT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_CELL_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    CELL_ID         VARCHAR(50) NOT NULL,
    PROJECT_PATH    VARCHAR(255) NOT NULL,
    PARAM_NAME_CD      VARCHAR(50) NOT NULL,
    VALUE           TEXT NULL,
    CAN_OVERRIDE    INT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );
CREATE TABLE PM_GLOBAL_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    PARAM_NAME_CD      VARCHAR(50) NOT NULL,
    PROJECT_PATH    VARCHAR(255) NOT NULL,
    VALUE           TEXT NULL,
    CAN_OVERRIDE    INT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );
	
CREATE TABLE PM_HIVE_DATA ( 
	DOMAIN_ID  	VARCHAR(50) NOT NULL,
	HELPURL    	VARCHAR(255) NULL,
	DOMAIN_NAME	VARCHAR(255) NULL,
	ENVIRONMENT_CD	VARCHAR(255) NULL,
	ACTIVE     	INT NULL ,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_HIVE_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    DOMAIN_ID         VARCHAR(50) NOT NULL,
    PARAM_NAME_CD    VARCHAR(50) NOT NULL,
    VALUE           TEXT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );

CREATE TABLE PM_PROJECT_DATA ( 
	PROJECT_ID  	VARCHAR(50) NOT NULL,
	PROJECT_NAME	VARCHAR(255) NULL,
	PROJECT_WIKI	VARCHAR(255) NULL,
	PROJECT_KEY 	VARCHAR(255) NULL,
	PROJECT_PATH	VARCHAR(255) NULL,
	PROJECT_DESCRIPTION	VARCHAR(2000) NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE     timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_PROJECT_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    PROJECT_ID        VARCHAR(50) NOT NULL,
    PARAM_NAME_CD    VARCHAR(50) NOT NULL,
    VALUE           TEXT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );

CREATE TABLE PM_PROJECT_USER_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    PROJECT_ID    VARCHAR(50) NOT NULL,
    USER_ID       VARCHAR(50) NOT NULL,
    PARAM_NAME_CD    VARCHAR(50) NOT NULL,
    VALUE           TEXT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );
CREATE TABLE PM_PROJECT_USER_ROLES ( 
	PROJECT_ID	VARCHAR(50) NOT NULL,
	USER_ID   	VARCHAR(50) NOT NULL,
	USER_ROLE_CD 	VARCHAR(255) NOT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);

CREATE TABLE PM_USER_LOGIN ( 
	USER_ID 		VARCHAR(50) NOT NULL,
	ATTEMPT_CD		VARCHAR(50) NOT NULL,
    ENTRY_DATE      timestamp NOT NULL,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)   
    );    
    	
CREATE TABLE PM_USER_DATA ( 
	USER_ID  	VARCHAR(50) NOT NULL,
	FULL_NAME	VARCHAR(255) NULL,
	PASSWORD 	VARCHAR(255) NULL,
	EMAIL	 	VARCHAR(255) NULL,
	PROJECT_PATH 	VARCHAR(255) NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_USER_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
    DATATYPE_CD      VARCHAR(50) NULL,
    USER_ID       VARCHAR(50) NOT NULL,
    PARAM_NAME_CD    VARCHAR(50) NOT NULL,
    VALUE           TEXT NULL,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
    CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
    );
CREATE TABLE PM_ROLE_REQUIREMENT ( 
	TABLE_CD   	VARCHAR(50) NOT NULL,
	COLUMN_CD	VARCHAR(50) NOT NULL,
	READ_HIVEMGMT_CD     	VARCHAR(50) NOT NULL,
	WRITE_HIVEMGMT_CD     	VARCHAR(50) NOT NULL,
	NAME_CHAR     	VARCHAR(2000),
    CHANGE_DATE     timestamp ,
    ENTRY_DATE     timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);	
CREATE TABLE PM_USER_SESSION ( 
	USER_ID 	VARCHAR(50) NOT NULL,
	SESSION_ID	VARCHAR(50) NOT NULL,
    EXPIRED_DATE         timestamp ,
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)   
    );
    
    
CREATE TABLE PM_APPROVALS ( 
	APPROVAL_ID     	VARCHAR(50) NOT NULL,
	APPROVAL_NAME	VARCHAR(255)  NULL,
	APPROVAL_DESCRIPTION        	VARCHAR(2000) NULL,
	APPROVAL_ACTIVATION_DATE      	timestamp NULL,
	APPROVAL_EXPIRATION_DATE         	timestamp NULL,
	OBJECT_CD		VARCHAR(50),
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_APPROVALS_PARAMS ( 
    ID                SERIAL PRIMARY KEY,
	APPROVAL_ID     	VARCHAR(50) NOT NULL,
	PARAM_NAME_CD  	VARCHAR(50) NOT NULL,
	VALUE       	TEXT NULL,
	ACTIVATION_DATE      	timestamp NULL,
	EXPIRATION_DATE         	timestamp NULL,
	DATATYPE_CD  	VARCHAR(50) NULL,
	OBJECT_CD		VARCHAR(50),
    CHANGE_DATE     timestamp ,
    ENTRY_DATE      timestamp ,
	CHANGEBY_CHAR   VARCHAR(50),
    STATUS_CD       VARCHAR(50)
	);
CREATE TABLE PM_PROJECT_REQUEST  ( 
    ID                SERIAL PRIMARY KEY,
	TITLE			VARCHAR(255) NULL,
	REQUEST_XML  	TEXT NOT NULL,
	CHANGE_DATE  	timestamp NULL,
	ENTRY_DATE   	timestamp NULL,
	CHANGEBY_CHAR	VARCHAR(50) NULL,
	STATUS_CD    	VARCHAR(50) NULL,
	PROJECT_ID   	VARCHAR(50) NULL,
	SUBMIT_CHAR  	VARCHAR(50) NULL
);    
CREATE INDEX PM_USER_LOGIN_IDX ON PM_USER_LOGIN(USER_ID, ENTRY_DATE);	
ALTER TABLE PM_USER_SESSION
	ADD  PRIMARY KEY (SESSION_ID, USER_ID);
ALTER TABLE PM_CELL_DATA
	ADD  PRIMARY KEY (CELL_ID, PROJECT_PATH);
ALTER TABLE PM_HIVE_DATA
	ADD  PRIMARY KEY (DOMAIN_ID);
ALTER TABLE PM_PROJECT_DATA
	ADD  PRIMARY KEY (PROJECT_ID);
ALTER TABLE PM_PROJECT_USER_ROLES
	ADD  PRIMARY KEY (PROJECT_ID, USER_ID, USER_ROLE_CD);
ALTER TABLE PM_ROLE_REQUIREMENT
	ADD  PRIMARY KEY (TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD);
ALTER TABLE PM_USER_DATA
	ADD  PRIMARY KEY (USER_ID); 
-- pm_user_session_arc create needs to happen after pm_user_session primary key is assigned, so that is copied also
CREATE TABLE pm_user_session_arc 
    (LIKE pm_user_session INCLUDING ALL);
ALTER TABLE pm_user_session_arc
    ADD COLUMN archived_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP;
INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('i2b2', 'i2b2 Admin', '9117d59a69dc49807671a51f10ab7f', 'A');
INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('AGG_SERVICE_ACCOUNT', 'AGG_SERVICE_ACCOUNT', '9117d59a69dc49807671a51f10ab7f', 'A');
INSERT INTO PM_HIVE_DATA (DOMAIN_ID, HELPURL, DOMAIN_NAME, ENVIRONMENT_CD, ACTIVE, STATUS_CD)
VALUES('i2b2', 'http://www.i2b2.org', 'i2b2demo', 'DEVELOPMENT', 1, 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('@', 'i2b2', 'ADMIN', 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_HIVE_DATA', '@', '@', 'ADMIN', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_HIVE_PARAMS', '@', '@', 'ADMIN', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_PROJECT_DATA', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_PROJECT_USER_ROLES', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_USER_DATA', '@', '@', 'ADMIN', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_PROJECT_PARAMS', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_PROJECT_USER_PARAMS', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_USER_PARAMS', '@', '@', 'ADMIN', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_CELL_DATA', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_CELL_PARAMS', '@', '@', 'MANAGER', NULL, NULL, NULL, NULL, 'A');
INSERT INTO PM_ROLE_REQUIREMENT(TABLE_CD, COLUMN_CD, READ_HIVEMGMT_CD, WRITE_HIVEMGMT_CD, NAME_CHAR, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('PM_GLOBAL_PARAMS', '@', '@', 'ADMIN', NULL, NULL, NULL, NULL, 'A');	
INSERT INTO PM_GLOBAL_PARAMS ( ID, DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
  VALUES('1', 'T', 'Predefined Global Params', '/',   '{ "label": "PM_COMPLEX_PASSWORD", "dataType": "T", "defaultValue": "<![CDATA[(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[)(;:}{,.><!@#$%^&+=])(?=\S+$).{8,}]]", "description": "Requires complex passwords when creating or resetting a password."},  { "label": "PM_EXPIRED_PASSWORD", "dataType": "T", defaultValue: "20", "description": "Password validity period (in days) before expiration."},  { "label": "PM_LOCKED_MAX_COUNT", "dataType": "T", "defaultValue": "10", "description": "Failed login attempt limit before lockout."},  { "label": "PM_LOCKED_WAIT_TIME", "dataType": "T", "defaultValue": "2", "description": "Lockout duration (in minutes) after failed login limit is exceeded."}, ])', NULL, now(), now(), NULL, 'A');
INSERT INTO PM_GLOBAL_PARAMS ( ID, DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
 VALUES('2', 'T', 'Predefined Project Params', '/',  '[  { "label": "Announcement", "dataType": "T", "description": "This is the project announcement displayed when the user selects a project after login"},  { "label": "Data Request Email Address", "dataType": "T", "defaultValue": "none@site.org", "description": "Recipient email for data request notifications."},  { "label": "Data Request Template", "dataType": "T", "defaultValue": "This user {{{USER_NAME}}} in project {{{PROJECT_ID}}} requested i2b2 request  entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, with the query master of {{{QUERY_MASTER_ID}}}. Check the status of the Data Request using the Data Request Manager plugin. ", "description": "Email content used in manager data request notifications."},  { "label": "Data Request Letter", "dataType": "T", "defaultValue": "Results of the i2b2 request entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, are available. Important notes about your data: - Total number of patients returned in your data request: {{{PATIENT_COUNT}}} - i2b2 reviewer: Only persons specifically authorized and selected (as listed at the top of this letter) can download these files. If additional user access is needed, please ensure the person is listed on your project IRB protocol and contact the i2b2 team. Specifically: - Remove all PHI from computer, laptop, or mobile device after analysis is completed. - Do NOT share PHI or PII with anyone who is not listed on the IRB protocol. Your guideline for the storage of Protected Health Information can be found at: https://www.site.com/guidelines_for_protecting_and_storing_phi.pdf *To download these files* - You must be logged onto your site These results are the data that was requested under the authority of the Institutional Review Board. The query resulting in this identified patient data is included at the end of this letter. A copy of this letter is kept on file and is available to the IRB in the event of an audit. Thank you, The i2b2 Team ", "description": "Email text sent to the user with data request submission details."},  { "label": "Data Request Subject", "dataType": "T", "defaultValue": "i2b2 Data Request", "description": "Email notification subject for data request submissions."} ]',  NULL, now(), now(), NULL, 'A');
INSERT INTO PM_GLOBAL_PARAMS ( ID, DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
 VALUES('3', 'T', 'Predefined User Params', '/',   '[{ "label": "PM_EXPIRED_PASSWORD", "dataType": "T", "description": "When the user changes expired password, system automatically adds row in PM_USER_PARAMS." }]',  NULL, now(), now(), NULL, 'A');





INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('demo', 'i2b2 User', '9117d59a69dc49807671a51f10ab7f', 'A');

INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('demo_obf', 'i2b2 Obfuscated User', '9117d59a69dc49807671a51f10ab7f', 'A');

INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('demo_mgr', 'i2b2 Manager User', '9117d59a69dc49807671a51f10ab7f', 'A');

INSERT INTO PM_PROJECT_DATA (PROJECT_ID, PROJECT_NAME, PROJECT_WIKI, PROJECT_PATH, STATUS_CD)
VALUES('Demo', 'i2b2 Demo', 'http://www.i2b2.org', '/Demo', 'A');


INSERT INTO PM_CELL_DATA (CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
  VALUES('CRC', '/', 'Data Repository', 'REST', 'http://localhost:9090/i2b2/services/QueryToolService/', 1, 'A');
INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
  VALUES('FRC', '/', 'File Repository ', 'SOAP', 'http://localhost:9090/i2b2/services/FRService/', 1, 'A');
INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
  VALUES('ONT', '/', 'Ontology Cell', 'REST', 'http://localhost:9090/i2b2/services/OntologyService/', 1, 'A');
INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
  VALUES('WORK', '/', 'Workplace Cell', 'REST', 'http://localhost:9090/i2b2/services/WorkplaceService/', 1, 'A');
INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
  VALUES('IM', '/', 'IM Cell', 'REST', 'http://localhost:9090/i2b2/services/IMService/', 1, 'A');


INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'AGG_SERVICE_ACCOUNT', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'AGG_SERVICE_ACCOUNT', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'AGG_SERVICE_ACCOUNT', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'AGG_SERVICE_ACCOUNT', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'i2b2', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'i2b2', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'i2b2', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'DATA_DEID', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'DATA_LDS', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'EDITOR', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo', 'DATA_PROT', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_obf', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_obf', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_mgr', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_mgr', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_mgr', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('Demo', 'demo_mgr', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE, CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'Demo', 'Data Request Template', 'This user {{{USER_NAME}}} in project {{{PROJECT_ID}}} requested i2b2 request
 entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, with the query master of {{{QUERY_MASTER_ID}}}.
Check the status of the Data Request using the Data Request Manager plugin.', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE,  CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'Demo', 'Data Request Email Address', 'email@site.org', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE,  CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'Demo', 'Data Request Subject', 'i2b2 Data Request', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE, CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'Demo', 'Data Request Letter', '"Results of the i2b2 request entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, are available.

Important notes about your data:
	- Total number of patients returned in your data request: {{{PATIENT_COUNT}}}
	- i2b2 reviewer:

Only persons specifically authorized and selected (as listed at the top of this letter) can download these files. If additional user access is needed, please ensure the person is listed on your project IRB protocol and contact the i2b2 team.

Specifically:
	- Remove all PHI from computer, laptop, or mobile device after analysis is completed.
	- Do NOT share PHI or PII with anyone who is not listed on the IRB protocol.

Your guideline for the storage of Protected Health Information can be found at: https://www.site.com/guidelines_for_protecting_and_storing_phi.pdf

*To download these files*
- You must be logged onto your site

These results are the data that was requested under the authority of the Institutional Review Board.  The query resulting in this identified patient data is included at the end of this letter.  A copy of this letter is kept on file and is available to the IRB in the event of an audit.

Thank you,

The i2b2 Team "', 'i2b2', 'H');

INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('act', 'i2b2 User', '9117d59a69dc49807671a51f10ab7f', 'A');
INSERT INTO PM_PROJECT_DATA (PROJECT_ID, PROJECT_NAME, PROJECT_WIKI, PROJECT_PATH, STATUS_CD)
VALUES('ACT', 'i2b2 ACT', 'http://www.i2b2.org', '/ACT', 'A');

-- INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
-- VALUES('AGG_SERVICE_ACCOUNT', 'AGG_SERVICE_ACCOUNT', '9117d59a69dc49807671a51f10ab7f', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'MANAGER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'DATA_DEID', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'DATA_LDS', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'EDITOR', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'act', 'DATA_PROT', 'A');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE, CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'ACT', 'Data Request Template', 'This user {{{USER_NAME}}} in project {{{PROJECT_ID}}} requested i2b2 request
 entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, with the query master of {{{QUERY_MASTER_ID}}}.
Check the status of the Data Request using the Data Request Manager plugin.', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE,  CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'ACT', 'Data Request Email Address', 'email@site.org', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE,  CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'ACT', 'Data Request Subject', 'i2b2 Data Request', 'i2b2', 'H');
INSERT INTO PM_PROJECT_PARAMS (DATATYPE_CD, PROJECT_ID, PARAM_NAME_CD, VALUE, CHANGEBY_CHAR, STATUS_CD) VALUES ('T', 'ACT', 'Data Request Letter', '"Results of the i2b2 request entitled - "{{{QUERY_NAME}}}", submitted on {{{QUERY_STARTDATE}}}, are available.

Important notes about your data:
	- Total number of patients returned in your data request: {{{PATIENT_COUNT}}}
	- i2b2 reviewer:

Only persons specifically authorized and selected (as listed at the top of this letter) can download these files. If additional user access is needed, please ensure the person is listed on your project IRB protocol and contact the i2b2 team.

Specifically:
	- Remove all PHI from computer, laptop, or mobile device after analysis is completed.
	- Do NOT share PHI or PII with anyone who is not listed on the IRB protocol.

Your guideline for the storage of Protected Health Information can be found at: https://www.site.com/guidelines_for_protecting_and_storing_phi.pdf

*To download these files*
- You must be logged onto your site

These results are the data that was requested under the authority of the Institutional Review Board.  The query resulting in this identified patient data is included at the end of this letter.  A copy of this letter is kept on file and is available to the IRB in the event of an audit.

Thank you,

The i2b2 Team "', 'i2b2', 'H');

-- INSERT INTO PM_CELL_DATA (CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
--   VALUES('CRC', '/', 'Data Repository', 'REST', 'http://localhost/i2b2/services/QueryToolService/', 1, 'A');
-- INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
--   VALUES('FRC', '/', 'File Repository ', 'SOAP', 'http://localhost/i2b2/services/FRService/', 1, 'A');
-- INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
--   VALUES('ONT', '/', 'Ontology Cell', 'REST', 'http://localhost/i2b2/services/OntologyService/', 1, 'A');
-- INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
--   VALUES('WORK', '/', 'Workplace Cell', 'REST', 'http://localhost/i2b2/services/WorkplaceService/', 1, 'A');
-- INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
--   VALUES('IM', '/', 'IM Cell', 'REST', 'http://localhost/i2b2/services/IMService/', 1, 'A');
