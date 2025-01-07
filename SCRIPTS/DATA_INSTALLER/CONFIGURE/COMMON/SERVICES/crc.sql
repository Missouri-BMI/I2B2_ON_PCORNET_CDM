USE SCHEMA {crc_schema};
DROP TABLE IF EXISTS patient_dimension;
DROP TABLE IF EXISTS visit_dimension;
DROP TABLE IF EXISTS provider_dimension;


update QT_QUERY_RESULT_TYPE
set classname = 'edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator'
where name = 'PATIENT_INOUT_XML';

update QT_BREAKDOWN_PATH
SET VALUE = '\\\\ACT_VISIT\\ACT\\Visit Details\\Visit type\\'
WHERE NAME = 'PATIENT_INOUT_XML';


UPDATE QT_BREAKDOWN_PATH
SET VALUE = REPLACE(VALUE, 'observation_fact', 'diagnosis_fact')
WHERE NAME = 'PATIENT_TOP20DIAG_XML';

UPDATE QT_BREAKDOWN_PATH
SET VALUE = REPLACE(VALUE, 'observation_fact', 'medication_fact')
WHERE NAME = 'PATIENT_TOP20MEDS_XML';