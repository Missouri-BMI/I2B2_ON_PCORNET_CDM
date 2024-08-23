USE SCHEMA REPORT;
CREATE OR REPLACE PROCEDURE generate_orphan_concepts()
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
DECLARE
    fact_tables RESULTSET DEFAULT (select table_name from information_schema.tables where lower(table_name) like lower('%_FACT') and lower(table_name) != 'tumor_fact');
    v_sqlStr TEXT DEFAULT '';  
    fact_cur CURSOR FOR fact_tables; 

BEGIN
    for r in fact_cur DO
         if (v_sqlStr != '') then
            v_sqlStr := v_sqlStr || '\nunion\n';
         end if;
         v_sqlStr := v_sqlStr || 'select concept_cd as CONCEPT_CD, ' || '''' || r.table_name || '''' || ' as FACT_TABLE' ||', COUNT(distinct patient_num) PATIENT_COUNT from i2b2data.' || r.table_name || ' Group by concept_cd';
    END FOR;
    
    -- create concepts
    execute immediate ('create or replace temp table CONCEPTS as \n' || v_sqlStr);
   
    -- create concept with paths
    CREATE OR REPLACE TEMP TABLE CONCEPTS_WITH_PATH as 
    SELECT concepts.*, cd.concept_path FROM CONCEPTS as concepts
    left join i2b2data.concept_dimension as cd
    using(concept_cd);
END;
$$
;