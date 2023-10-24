CREATE OR REPLACE TABLE PRIVATE_VISIT_FACT ( 
	ENCOUNTER_NUM  		INT NOT NULL,
	PATIENT_NUM    		INT NOT NULL,
	CONCEPT_CD     		VARCHAR(50) NOT NULL,
	PROVIDER_ID    		VARCHAR(50) NOT NULL,
	START_DATE     		TIMESTAMP NOT NULL,
	MODIFIER_CD    		VARCHAR(100) default '@' NOT NULL,
	INSTANCE_NUM		INT default (1) NOT NULL,
	VALTYPE_CD     		VARCHAR(50) NULL,
	TVAL_CHAR      		VARCHAR(255) NULL,
	NVAL_NUM       		DECIMAL(18,5) NULL,
	VALUEFLAG_CD   		VARCHAR(50) NULL,
	QUANTITY_NUM   		DECIMAL(18,5) NULL,
	UNITS_CD       		VARCHAR(50) NULL,
	END_DATE       		TIMESTAMP NULL,
	LOCATION_CD    		VARCHAR(50) NULL,
	OBSERVATION_BLOB	TEXT NULL,
	CONFIDENCE_NUM 		DECIMAL(18,5) NULL,
	UPDATE_DATE    		TIMESTAMP NULL,
	DOWNLOAD_DATE  		TIMESTAMP NULL,
	IMPORT_DATE    		TIMESTAMP NULL,
	SOURCESYSTEM_CD		VARCHAR(50) NULL, 
    UPLOAD_ID         	INT NULL,
    TEXT_SEARCH_INDEX   NUMBER(38, 0) NOT NULL
	)
;
create or replace sequence visit_text_index;

-- visit type
insert into PRIVATE_VISIT_FACT (
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    ENCOUNTER_NUM, 
    PATIENT_NUM, 
    concat('VISIT|TYPE:', COALESCE(inout_cd, 'UN')),
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_DATE()), 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    coalesce(END_DATE, null),
    '@', 
    null, 
    null, 
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    $cdm_version,                                                                    
    null, 
    visit_text_index.nextVal 
from DEID_VISIT_DIMENSION as dim;        



-- Length of stay
insert into PRIVATE_VISIT_FACT (                            
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    encounter_num, 
    PATIENT_NUM, 
    CASE
        WHEN LENGTH_OF_STAY =  0 THEN  concat('VISIT|LENGTH:', '0')
        WHEN LENGTH_OF_STAY =  1 THEN  concat('VISIT|LENGTH:', '1')
        WHEN LENGTH_OF_STAY =  2 THEN  concat('VISIT|LENGTH:', '2')
        WHEN LENGTH_OF_STAY =  3 THEN  concat('VISIT|LENGTH:', '3')
        WHEN LENGTH_OF_STAY =  4 THEN  concat('VISIT|LENGTH:', '4')
        WHEN LENGTH_OF_STAY =  5 THEN  concat('VISIT|LENGTH:', '5')
        WHEN LENGTH_OF_STAY =  6 THEN  concat('VISIT|LENGTH:', '6')
        WHEN LENGTH_OF_STAY =  7 THEN  concat('VISIT|LENGTH:', '7')
        WHEN LENGTH_OF_STAY =  8 THEN  concat('VISIT|LENGTH:', '8')
        WHEN LENGTH_OF_STAY =  9 THEN  concat('VISIT|LENGTH:', '9')
        WHEN LENGTH_OF_STAY =  10 THEN  concat('VISIT|LENGTH:', '10')
        WHEN LENGTH_OF_STAY >  10 THEN  concat('VISIT|LENGTH:', '>10')
        ELSE concat('VISIT|LENGTH:', '0')
    END,
    PROVIDER_ID, 
    coalesce(START_DATE, CURRENT_DATE()), 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    coalesce(END_DATE, null),
    '@', 
    null, 
    null, 
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    $cdm_version,                                            
    null, 
    visit_text_index.nextVal 
from DEID_VISIT_DIMENSION as dim;

-- CREATE VIEW
create or replace view  DEID_VISIT_FACT as 
select * from PRIVATE_VISIT_FACT;         



