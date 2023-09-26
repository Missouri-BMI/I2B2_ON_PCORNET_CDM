-- CRC SCHEMA IS HARD CODED TO i2b2data
-- ONT SCHEMA IS HARD CODED TO i2b2metadata
-- ONTOLOGY TABLE NAME IS HARD CODED TO 'GENERAL_OBSERVATIONS' AND DISPLAY NAME TO 'GENERAL OBSERVATIONS

CREATE OR REPLACE SCHEMA REPORT;

USE SCHEMA REPORT;

-- create concept with paths
execute immediate $$
DECLARE
    fact_tables RESULTSET;
    query VARCHAR DEFAULT
        'select table_name from information_schema.tables 
        where lower(table_name) like lower(''%_FACT'')';
    v_sqlStr VARCHAR DEFAULT '';
BEGIN

    fact_tables := (EXECUTE IMMEDIATE :query);
    let fact_cur CURSOR FOR fact_tables;
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

-- CREATE ONTLOGY CONCEPTS WITH PATH
execute immediate $$
DECLARE
    ont_tables RESULTSET;
    query VARCHAR DEFAULT 'select c_table_name from i2b2metadata.table_access where c_visualattributes like ''%A%''';
    v_sqlStr VARCHAR DEFAULT '';
BEGIN
    
    ont_tables := (EXECUTE IMMEDIATE :query);
    let ont_cur CURSOR FOR ont_tables;
    for r in ont_cur DO
         if (v_sqlStr != '') then
            v_sqlStr := v_sqlStr || '\nunion\n';
         end if;
         v_sqlStr := v_sqlStr || 'SELECT C_FULLNAME, C_FACTTABLECOLUMN FROM i2b2metadata.' || r.c_table_name || '\n where lower(C_FACTTABLECOLUMN) like ''%concept_cd%'' and                     lower(C_COLUMNNAME) = ''concept_path''';
    END FOR;
    
    -- create concepts with paths
    execute immediate ('create or replace TEMP TABLE ONT_CONCEPTS_WITH_PATH as \n' || v_sqlStr);
    
END;
$$
;
 
CREATE OR REPLACE TEMP TABLE CDMGAPS AS 
    SELECT * from CONCEPTS_WITH_PATH    as ccp
    left join ONT_CONCEPTS_WITH_PATH    as ocp
    on ccp.concept_path = ocp.c_fullname;

--NOT MAPPED: patient more than 10
-- FACT SPLIT: 50

CREATE OR REPLACE TABLE NOT_MAPPED AS 
    select 
        ROW_NUMBER() OVER ( PARTITION BY FACT_TABLE, split_part(CONCEPT_CD, ':', 1) ORDER BY CONCEPT_CD) as rn
        ,'\\GENERAL OBSERVATIONS' || '\\' ||  upper(split_part(lower(FACT_TABLE), '_fact', 0)) || '\\' || upper(split_part(lower(CONCEPT_CD), ':', 0)) || '\\' || iff(mod(rn, 50) = 0, ((floor(rn / 50) * 50)) - 50 + 1, ((floor(rn / 50) + 1) * 50) - 50 + 1) :: VARCHAR || '~' || iff(mod(rn, 50) = 0, (floor(rn / 50) * 50), ((floor(rn / 50) + 1) * 50)) :: VARCHAR || '\\' || CONCEPT_CD || '\\' as CONCEPT_PATH
        ,CONCEPT_CD
        , FACT_TABLE
        , PATIENT_COUNT 
    FROM CDMGAPS 
    where concept_path is null 
    and PATIENT_COUNT >= 10
;

--NOT MAPPED ACCORDINGLY:  patient more than 10
CREATE OR REPLACE TABLE NOT_MAPPED_ACCORDINGLY AS 
    select 
        CONCEPT_PATH
        , CONCEPT_CD
        , FACT_TABLE
        , C_FACTTABLECOLUMN
        , PATIENT_COUNT 
    FROM CDMGAPS 
    WHERE 
        concept_path is not null 
        and PATIENT_COUNT >= 10 
        and c_fullname is not null 
        and lower(fact_table) != lower(split_part(c_facttablecolumn, '.concept_cd', 0))
;