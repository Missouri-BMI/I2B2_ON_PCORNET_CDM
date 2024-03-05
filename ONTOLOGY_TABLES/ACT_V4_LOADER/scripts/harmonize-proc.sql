CREATE OR REPLACE PROCEDURE harmonize_proc(

)
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
DECLARE
    results RESULTSET;
    query VARCHAR DEFAULT 'select c_table_cd, c_table_name from table_access';
    tableName VARCHAR;
    ont_table_cd VARCHAR;
    query_str VARCHAR DEFAULT '';
    fact_table_column VARCHAR DEFAULT 'concept_cd';
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    
    for record in cur do
        ont_table_cd := record.c_table_cd;
        case (ont_table_cd)
            when 'ACT_DEMO' then 
            
            fact_table_column := 'demographic_fact.concept_cd';
        --- modify age
        
        --- delete race, sex, hispanic

        --- add gpc 

        --- modify combine diagnosis, procedure, lab patient counts concept
            execute immediate ('update ' || r.c_table_name || ' set c_facttablecolumn = \'medication_fact.concept_cd\' where c_fullname=\'\\ACT\\Demographics\\Patient Counts\\One Medication\\\'');
            execute immediate ('update ' || r.c_table_name || ' set c_facttablecolumn = \'procedure_fact.concept_cd\' where c_fullname=\'\\ACT\\Demographics\\Patient Counts\\One Procedure\\\'');
            execute immediate ('update ' || r.c_table_name || ' set c_facttablecolumn = \'lab_fact.concept_cd\' where c_fullname=\'\\ACT\\Demographics\\Patient Counts\\One Lab\\\'');
            execute immediate ('update ' || r.c_table_name || ' set c_facttablecolumn = \'diagnosis.concept_cd\' where c_fullname=\'\\ACT\\Demographics\\Patient Counts\\One Diagnosis\\\'');



            when 'ACT_VISIT' then fact_table_column := 'visit_fact.concept_cd';

            when 'ACT_DX_ICD10_2018' then fact_table_column := 'diagnosis_fact.concept_cd';
            when 'ACT_DX_10_9' then fact_table_column := 'diagnosis_fact.concept_cd';
            when 'ACT_DX_ICD9_2018' 
            then 
                fact_table_column := 'diagnosis_fact.concept_cd';
                execute immediate ('update ' || r.c_table_name || ' set c_visualattributes = \'FH\' where c_name like \'630-677.99 Complications Of Pregnancy, Childbirth, And The Puerperium\'');


            when 'ACT_PX_CPT_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_HCPCS_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_ICD10_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_ICD9_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_VAX' then fact_table_column := 'procedure_fact.concept_cd';

            when 'ACT_MED_ALPHA_2018' then fact_table_column := 'medication_fact.concept_cd';
            when 'ACT_MED_VA_2018' then fact_table_column := 'medication_fact.concept_cd';

            when 'ACT_LAB_LOINC_2018' then fact_table_column := 'lab_fact.concept_cd';
            when 'ACT_LAB' then fact_table_column := 'lab_fact.concept_cd';
            
            when 'ACT_COVID_V1' then fact_table_column := 'covid_fact.concept_cd';
            
            when 'ACT_VITAL_SIGNS' 
            then 
                fact_table_column := 'vital_fact.concept_cd';
                execute immediate ('update ' || r.c_table_name || ' set c_metadataxml = replace(c_metadataxml,\'>gm<\', \'>kg<\') where lower(c_name) like \'body weight%\'');
           
            when 'ACT_SDOH' then fact_table_column := 'sdoh_fact.concept_cd';
            when 'ACT_ZIPCODE' then fact_table_column := 'sdoh_fact.concept_cd';
           
            when 'ACT_RESEARCH' then fact_table_column := 'concept_cd';
            ELSE fact_table_column := 'concept_cd';
        end;
        execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'' || fact_table_column || '\'');
        execute immediate ('update table_access set c_facttablecolumn = \'' || fact_table_column || '\'' ||' where c_table_cd = \'' || ont_table_cd || '\'');
    end for;

END;
$$
;