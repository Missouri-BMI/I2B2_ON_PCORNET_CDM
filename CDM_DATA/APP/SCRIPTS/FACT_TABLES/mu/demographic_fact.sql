create or replace sequence demographic_text_index;

CREATE OR REPLACE TABLE PRIVATE_DEMOGRAPHIC_FACT ( 
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

-- demographic hispanic

insert into PRIVATE_DEMOGRAPHIC_FACT (
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    -1, 
    dim.PATIENT_NUM, 
    concat('DEM|HISP:', COALESCE(dim.HISPANIC, 'NI')),
    '@', 
    CURRENT_TIMESTAMP, 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    $cdm_version,                                                                    
    null, 
    demographic_text_index.nextVal 
from DEID_PATIENT_DIMENSION as dim;        



-- demographic race
insert into PRIVATE_DEMOGRAPHIC_FACT (                            
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    -1, 
    dim.PATIENT_NUM, 
    CASE
        WHEN dim.RACE_CD =  'American Indian or Alaska Native' THEN  concat('DEM|RACE:', '01')
        WHEN dim.RACE_CD =  'Asian' THEN  concat('DEM|RACE:', '02')
        WHEN dim.RACE_CD =  'Black or African American' THEN  concat('DEM|RACE:', '03')
        WHEN dim.RACE_CD =  'Native Hawaiian or Other Pacific Islander' THEN  concat('DEM|RACE:', '04')
        WHEN dim.RACE_CD =  'White' THEN  concat('DEM|RACE:', '05')
        WHEN dim.RACE_CD =  'Multiple race' THEN  concat('DEM|RACE:', '06')
        WHEN dim.RACE_CD =  'Refuse to answer' THEN  concat('DEM|RACE:', '07')
        WHEN dim.RACE_CD =  'No information' THEN  concat('DEM|RACE:', 'NI')
        WHEN dim.RACE_CD =  'Unknown' THEN  concat('DEM|RACE:', 'UN')
        WHEN dim.RACE_CD =  'Other' THEN  concat('DEM|RACE:', 'OT')
        ELSE concat('DEM|RACE:', 'NI')
    END,
    '@', 
    CURRENT_TIMESTAMP, 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    $cdm_version,                                            
    null, 
    demographic_text_index.nextVal 
from DEID_PATIENT_DIMENSION as dim; 


insert into PRIVATE_DEMOGRAPHIC_FACT (      
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    -1, 
    dim.PATIENT_NUM, 
    concat('DEM|SEX:', COALESCE(dim.SEX_CD, 'NI')),
    '@', 
    CURRENT_TIMESTAMP, 
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    $cdm_version,                                  
    null, 
    demographic_text_index.nextVal 
from DEID_PATIENT_DIMENSION as dim;          



-- demographic vital status
insert into PRIVATE_DEMOGRAPHIC_FACT (            
  ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, VALTYPE_CD, 
  TVAL_CHAR, NVAL_NUM, VALUEFLAG_CD, QUANTITY_NUM, UNITS_CD, END_DATE, LOCATION_CD, OBSERVATION_BLOB, CONFIDENCE_NUM,
  UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID, TEXT_SEARCH_INDEX)
select
    -1, 
    dim.PATIENT_NUM, 
    CASE
        WHEN VITAL_STATUS_CD = 'Y' THEN 'DEM|VITAL STATUS:D'
        ELSE '@'
    END,
    '@', 
    CURRENT_TIMESTAMP, -- no data
    '@',
    1, 
    '',
    '', 
    null, 
    '',
    null, 
    '@', 
    null, 
    '@', 
    null, 
    null, 
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    $cdm_version,    
    null, 
    demographic_text_index.nextVal 
from DEID_PATIENT_DIMENSION as dim; 

delete from PRIVATE_DEMOGRAPHIC_FACT where concept_cd = '@';

-- CREATE VIEW
create or replace view  DEID_DEMOGRAPHIC_FACT as 
select * from PRIVATE_DEMOGRAPHIC_FACT;