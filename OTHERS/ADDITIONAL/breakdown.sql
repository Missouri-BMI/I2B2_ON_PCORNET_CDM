SET SEARCH_PATH TO i2b2demodata;
TRUNCATE QT_BREAKDOWN_PATH;
INSERT 
    INTO QT_BREAKDOWN_PATH
        (NAME,VALUE,CREATE_DATE)
    VALUES 
        ('PATIENT_GENDER_COUNT_XML','\\ACT_DEMO\ACT\Demographics\Sex\',now());
INSERT
    INTO QT_BREAKDOWN_PATH
        (NAME,VALUE,CREATE_DATE) 
    VALUES 
        ('PATIENT_RACE_COUNT_XML','\\ACT_DEMO\ACT\Demographics\Race\',now());
INSERT 
    INTO QT_BREAKDOWN_PATH
        (NAME,VALUE,CREATE_DATE) 
    VALUES 
        ('PATIENT_VITALSTATUS_COUNT_XML','\\ACT_DEMO\ACT\Demographics\Vital Status\',now());
INSERT 
    INTO QT_BREAKDOWN_PATH
        (NAME,VALUE,CREATE_DATE) 
    VALUES 
        ('PATIENT_AGE_COUNT_XML','\\ACT_DEMO\ACT\Demographics\Age\',now());
INSERT 
    INTO qt_breakdown_path 
        (name, value, create_date)
    VALUES 
        ('PATIENT_LOS_XML','select length_of_stay as patient_range, count(distinct a.PATIENT_num) as patient_count from visit_dimension a, DX b where a.patient_num = b.patient_num group by a.length_of_stay order by 1', CURRENT_TIMESTAMP);
INSERT 
    INTO qt_breakdown_path 
        (name, value, create_date) 
    VALUES 
        ('PATIENT_TOP20MEDS_XML','select b.name_char as patient_range, count(distinct a.patient_num) as patient_count from prescribing_fact a, concept_dimension b, DX c where a.concept_cd = b.concept_cd and concept_path like ''\\ACT\\Medications\\%'' and a.patient_num = c.patient_num   group by name_char order by patient_count desc limit 20', CURRENT_TIMESTAMP);
INSERT 
    INTO qt_breakdown_path 
        (name, value, create_date) 
    VALUES 
        ('PATIENT_TOP20DIAG_XML','select b.name_char as patient_range, count(distinct a.patient_num) as patient_count from diagnosis_fact a, concept_dimension b, DX c where a.concept_cd = b.concept_cd and concept_path like ''\\ACT\\Diagnosis\\%'' and a.patient_num = c.patient_num   group by name_char order by patient_count desc limit 20', CURRENT_TIMESTAMP);
INSERT 
    INTO qt_breakdown_path 
        (name, value, create_date) 
    VALUES 
        ('PATIENT_INOUT_XML','select INOUT_CD as patient_range, count(distinct a.patient_num) as patient_count from visit_dimension a, DX b where a.patient_num = b.patient_num group by a.INOUT_CD order by 1', CURRENT_TIMESTAMP);
