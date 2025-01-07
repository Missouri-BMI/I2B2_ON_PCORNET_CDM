-- CRC SCHEMA IS HARD CODED TO i2b2data
-- ONT SCHEMA IS HARD CODED TO i2b2metadata
-- ONTOLOGY TABLE NAME IS HARD CODED TO 'MISCELLANEOUS_OBSERVATIONS' AND DISPLAY NAME TO 'MISCELLANEOUS OBSERVATIONS'
USE SCHEMA REPORT;
CREATE OR REPLACE PROCEDURE generate_cdm_gaps()
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
BEGIN
    
    CREATE OR REPLACE TABLE CDMGAPS AS 
        SELECT * from CONCEPTS_WITH_PATH    as ccp
        left join ONT_CONCEPTS_WITH_PATH    as ocp
        on ccp.concept_path = ocp.c_fullname;

    CREATE OR REPLACE TABLE NOT_MAPPED AS 
        select 
            ROW_NUMBER() OVER ( PARTITION BY FACT_TABLE, split_part(CONCEPT_CD, ':', 1) ORDER BY CONCEPT_CD) as rn
            ,'\\MISCELLANEOUS OBSERVATIONS' || '\\' ||  upper(split_part(lower(FACT_TABLE), '_fact', 0)) || '\\' || upper(split_part(lower(CONCEPT_CD), ':', 0)) || '\\' || iff(mod(rn, 50) = 0, ((floor(rn / 50) * 50)) - 50 + 1, ((floor(rn / 50) + 1) * 50) - 50 + 1) :: VARCHAR || '~' || iff(mod(rn, 50) = 0, (floor(rn / 50) * 50), ((floor(rn / 50) + 1) * 50)) :: VARCHAR || '\\' || CONCEPT_CD || '\\' as CONCEPT_PATH
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
END;
$$
;

