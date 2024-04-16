CREATE OR REPLACE PROCEDURE handle_age_at_visit(tableName varchar)
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
DECLARE
    results RESULTSET;
    query TEXT DEFAULT 'select c_fullname, split_part(c_fullname, \'\\\\\', -2) as fullT, split_part(fullT, \' \', 1) as num, split_part(fullT, \' \', 2) as dPart  from ' || tableName || ' where c_fullname like \'%Age at visit%\' and c_facttablecolumn = \'encounter_num\'';
    dateComp VARCHAR;
    updateQuery TEXT default '';
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
        query := 'select PATIENT_NUM from PATIENT_DIMENSION join VISIT_DIMENSION using (PATIENT_NUM) WHERE START_DATE ';
        if(contains(record.dpart,'months')) then
            dateComp := 'MM';
        else
            dateComp := 'YY';
        end if;
        if (contains(record.num, '>=')) then
            query := query || '>= DATEADD(YY, '|| record.DPART ||', BIRTH_DATE)';
          
        elseif (contains(record.num, '-')) then
            let firstP := split_part(record.num, '-', 1);
            let secondP := split_part(record.num, '-', 2);
            query := query || ' BETWEEN ' || 'DATEADD('||dateComp||', ' || firstP || ', BIRTH_DATE) ';
            query := query || 'AND DATEADD(DD, -1, ' || 'DATEADD('||dateComp||', ' || ((secondP :: int) + 1) :: varchar || ', BIRTH_DATE))';
        else 
            query := query || ' BETWEEN ' || 'DATEADD('||dateComp||', ' || record.num || ', BIRTH_DATE) ';
            query := query || 'AND DATEADD(DD, -1, ' || 'DATEADD('||dateComp||', ' || ((record.num :: int) + 1) :: varchar || ', BIRTH_DATE))';
        end if;
        
        updateQuery := ('update ' || tableName || ' set c_dimcode = \'' || query || '\' where c_fullname=\''||replace(record.c_fullname,'\\','\\\\') ||'\'');
        execute immediate :updateQuery;
    end for;
END;
$$
;


create or replace procedure handle_visit_details(tableName varchar)
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
begin
    -- visit type
    update identifier(:tableName) 
        set c_basecode = concat('VISIT|TYPE:', c_basecode)
        , c_facttablecolumn = 'visit_fact.concept_cd'
        , c_tablename = 'concept_dimension'
        , c_columnname = 'concept_path'
        , c_columndatatype = 'T'
        , c_operator = 'LIKE'
        , c_dimcode = c_fullname
    where c_fullname like '%\\ACT\\Visit Details\\Visit type\\%';

    insert into concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
        select
            c_dimcode
            , c_basecode
            , c_name
            , null
            , current_timestamp
            , current_timestamp
            , current_timestamp
            , 'ACT'
            , null
        from identifier(:tableName)
        where c_fullname like  '%\\ACT\\Visit Details\\Visit type\\%' and c_basecode is not null; 

    -- lenght of stay
    update identifier(:tableName)
    set 
        c_basecode = concat('VISIT|LENGTH:', c_basecode)
        , c_facttablecolumn = 'visit_fact.concept_cd'
        , c_tablename = 'concept_dimension'
        , c_columnname = 'concept_path'
        , c_columndatatype = 'T'
        , c_operator = 'LIKE'
        , c_dimcode = c_fullname
    where c_fullname like '%\\ACT\\Visit Details\\Length of stay\\%';

    insert into concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
    select 
        c_fullname
        , c_basecode
        , c_name
        , null
        , current_timestamp
        , current_timestamp
        , current_timestamp
        , 'ACT'
        , null
    from identifier(:tableName)
    where c_fullname like '%\\ACT\\Visit Details\\Length of stay\\%' and c_basecode is not null; 
                
end;
$$
;

create or replace procedure handle_covid_ont(tableName varchar)
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
begin
    ---visit type in covid
    update identifier(:tableName) 
    set c_basecode = 
    CASE 
        WHEN c_dimcode = 'O' THEN concat('VISIT|TYPE:', 'AV')
        WHEN c_dimcode = 'I' THEN concat('VISIT|TYPE:', 'IP')
        WHEN c_dimcode = 'E' THEN concat('VISIT|TYPE:', 'ED')
        WHEN c_dimcode = 'EI' THEN concat('VISIT|TYPE:', 'EI')
    end
    , c_facttablecolumn = 'covid_fact.concept_cd'
    , c_tablename = 'concept_dimension'
    , c_columnname = 'concept_path'
    , c_columndatatype = 'T'
    , c_operator = 'LIKE'
    , c_dimcode = c_fullname
    where c_columnname = 'inout_cd';
    
    --
    update identifier(:tableName) 
    set c_facttablecolumn = 'patient_num'
    where c_columnname = 'patient_num';

    --
    insert into concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
        select
            c_dimcode
            , c_basecode
            , c_name
            , null
            , current_timestamp
            , current_timestamp
            , current_timestamp
            , 'ACT'
            , null
        from identifier(:tableName) 
        WHERE C_BASECODE LIKE 'VISIT|TYPE:%';         
end;
$$
;

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
                execute immediate ('update ' || record.c_table_name || ' set C_DIMCODE = replace(c_dimcode, \'NOW()\', \'CURRENT_DATE()\')');
                --- delete race, sex, hispanic

                --- add gpc 

                --- modify combine diagnosis, procedure, lab patient counts concept
                execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'medication_fact.concept_cd\' where c_fullname=\'\\\\ACT\\\\Demographics\\\\Patient Counts\\\\One Medication\\\\\'');
                execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'procedure_fact.concept_cd\' where c_fullname=\'\\\\ACT\\\\Demographics\\\\Patient Counts\\\\One Procedure\\\\\'');
                execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'lab_fact.concept_cd\' where c_fullname=\'\\\\ACT\\\\Demographics\\\\Patient Counts\\\\One Lab\\\\\'');
                execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'DIAGNOSIS_FACT.concept_cd\' where c_fullname=\'\\\\ACT\\\\Demographics\\\\Patient Counts\\\\One Diagnosis\\\\\'');

            when 'ACT_VISIT' then 
                fact_table_column := 'visit_fact.concept_cd';
                tableName := record.c_table_name;
                --age at visit
                call handle_age_at_visit(:tableName);
                call handle_visit_details(:tableName);
                
            when 'ACT_DX_ICD10_2018' then fact_table_column := 'diagnosis_fact.concept_cd';
            when 'ACT_DX_10_9' then fact_table_column := 'diagnosis_fact.concept_cd';
            when 'ACT_DX_ICD9_2018' 
            then 
                fact_table_column := 'diagnosis_fact.concept_cd';
                execute immediate ('update ' || record.c_table_name || ' set c_visualattributes = \'FH\' where c_name like \'630-677.99 Complications Of Pregnancy, Childbirth, And The Puerperium\'');


            when 'ACT_PX_CPT_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_HCPCS_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_ICD10_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_PX_ICD9_2018' then fact_table_column := 'procedure_fact.concept_cd';
            when 'ACT_VAX' then fact_table_column := 'procedure_fact.concept_cd';

            when 'ACT_MED_ALPHA_2018' then fact_table_column := 'medication_fact.concept_cd';
            when 'ACT_MED_VA_2018' then fact_table_column := 'medication_fact.concept_cd';

            when 'ACT_LAB_LOINC_2018' then fact_table_column := 'lab_fact.concept_cd';
            when 'ACT_LAB' then fact_table_column := 'lab_fact.concept_cd';
            
            when 'ACT_COVID_V1' then 
                fact_table_column := 'covid_fact.concept_cd';
                tableName := record.c_table_name;
                call handle_covid_ont(:tableName);
                
            when 'ACT_VITAL_SIGNS' 
            then 
                fact_table_column := 'vital_fact.concept_cd';
                execute immediate ('update ' || record.c_table_name || ' set c_metadataxml = replace(c_metadataxml,\'>gm<\', \'>kg<\') where lower(c_name) like \'body weight%\'');
           
            when 'ACT_SDOH' then fact_table_column := 'sdoh_fact.concept_cd';
            when 'ACT_ZIPCODE' then fact_table_column := 'sdoh_fact.concept_cd';
           
            when 'ACT_RESEARCH' then fact_table_column := 'concept_cd';
            ELSE fact_table_column := 'concept_cd';
        end;

        execute immediate ('update ' || record.c_table_name || ' set c_facttablecolumn = \'' || fact_table_column || '\' where c_facttablecolumn = \'concept_cd\'');
        execute immediate ('update table_access set c_facttablecolumn = \'' || fact_table_column || '\'' ||' where c_table_cd = \'' || ont_table_cd || '\'');
    end for;

END;
$$
;