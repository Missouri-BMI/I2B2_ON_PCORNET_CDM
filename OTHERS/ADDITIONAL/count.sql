set search_path to i2b2metadata;

select runtotalnum('observation_fact', 'i2b2demodata', 'NCATS_DEMOGRAPHICS');
select runtotalnum('observation_fact', 'i2b2demodata', 'NCATS_VISIT_DETAILS');


select runtotalnum('diagnosis_fact', 'i2b2demodata', 'ACT_ICD9CM_DX_2018AA');
select runtotalnum('diagnosis_fact', 'i2b2demodata', 'ACT_ICD10CM_DX_2018AA');

select runtotalnum('procedure_fact', 'i2b2demodata', 'ACT_ICD9CM_PX_2018AA');
select runtotalnum('procedure_fact', 'i2b2demodata', 'ACT_ICD10PCS_PX_2018AA');
select runtotalnum('procedure_fact', 'i2b2demodata', 'ACT_CPT_PX_2018AA');
select runtotalnum('procedure_fact', 'i2b2demodata', 'I2B2_CPT4');
select runtotalnum('procedure_fact', 'i2b2demodata', 'ACT_HCPCS_PX_2018AA');

select runtotalnum('prescribing_fact', 'i2b2demodata', 'ACT_MED_VA_V2_092818');
select runtotalnum('prescribing_fact', 'i2b2demodata', 'ACT_MED_ALPHA_V2_092818');

select runtotalnum('lab_fact', 'i2b2demodata', 'ACT_LOINC_LAB_2018AA');
select runtotalnum('lab_fact', 'i2b2demodata', 'NCATS_LABS');

select runtotalnum('vital_fact', 'i2b2demodata', 'VITAL_SIGNS');

-- fixed some issues in runtotalnum. manully updated some columns which could not fixed
update table_access set c_totalnum = null where c_table_name = 'VITAL_SIGNS';
update table_access set c_totalnum = null where c_table_name = 'NCATS_DEMOGRAPHICS';
update table_access set c_totalnum = null where c_table_name = 'NCATS_VISIT_DETAILS';