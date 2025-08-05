set target_db = 'i2b2_dev';
set target_schema = $target_db || '.' || 'i2b2data';

use database identifier($target_db);
use schema identifier($target_schema);

CREATE OR REPLACE PROCEDURE tumor_columns()
RETURNS VARCHAR
LANGUAGE SQL
AS 
DECLARE
  column_names VARCHAR DEFAULT '';  
  cur CURSOR FOR 
    SELECT column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
    where 
        table_name = 'DEID_TUMOR' 
        and table_schema = 'CDM'
        and lower(column_name) like lower('%_N%') 
        and lower(column_name) not like lower('RAW_%')
        and data_type = 'TEXT';
BEGIN

    FOR curRecord IN cur DO
        if (curRecord.column_name in ('DATE_CASE_INITIATED_N2085','DATE_CASE_COMPLETED_N2090', 'PATID')) then
            continue;
        elseif (column_names = '') then
            column_names := curRecord.column_name;
        else
            column_names := column_names || ', ' || curRecord.column_name;
        end if;
    end for;
    return column_names;
END;

CREATE OR REPLACE PROCEDURE create_tumor_fact()
RETURNS VARCHAR NULL
LANGUAGE SQL
AS
DECLARE
    column_names VARCHAR DEFAULT '';  
    v_sql VARCHAR;
BEGIN
    call tumor_columns() into :column_names;
    v_sql := 
        $$
        CREATE OR REPLACE TABLE i2b2_dev.i2b2data.TUMOR_FACT AS
        SELECT
            -1::NUMBER(38,0) AS ENCOUNTER_NUM,
            PATID::NUMBER(38,0) AS PATIENT_NUM,
            CONCAT('NAACCR|', SPLIT_PART(concept, '_N', -1), ':', COALESCE(concept_cd, '')) AS CONCEPT_CD,
            '@' AS PROVIDER_ID,
            DATE_CASE_INITIATED_N2085::TIMESTAMP AS START_DATE,
            '@' AS MODIFIER_CD,
            1 AS INSTANCE_NUM,
            '' AS VALTYPE_CD,
            '' AS TVAL_CHAR,
            CAST(NULL AS INTEGER) AS NVAL_NUM,
            '' AS VALUEFLAG_CD,
            CAST(NULL AS INTEGER) AS QUANTITY_NUM,
            '@' AS UNITS_CD,
            CAST(DATE_CASE_COMPLETED_N2090 AS TIMESTAMP) AS END_DATE,
            '@' AS LOCATION_CD,
            CAST(NULL AS TEXT) AS OBSERVATION_BLOB,
            CAST(NULL AS INTEGER) AS CONFIDENCE_NUM,
            CURRENT_TIMESTAMP AS UPDATE_DATE,
            CURRENT_TIMESTAMP AS DOWNLOAD_DATE,
            CURRENT_TIMESTAMP AS IMPORT_DATE,
            CAST(NULL AS VARCHAR(50)) AS SOURCESYSTEM_CD,
            CAST(NULL AS INTEGER) AS UPLOAD_ID
        FROM DEIDENTIFIED_PCORNET_CDM.CDM.DEID_TUMOR
        UNPIVOT (
            concept_cd FOR concept IN ($column_names)
        )
        ORDER BY PATID;
        $$;
    execute immediate :v_sql;
    return v_sql;
END;

call create_tumor_fact();