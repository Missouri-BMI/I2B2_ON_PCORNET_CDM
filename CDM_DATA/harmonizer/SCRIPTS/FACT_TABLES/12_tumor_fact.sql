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
    v_sql := 'CREATE OR REPLACE TABLE i2b2_dev.i2b2data.TUMOR_FACT AS ' || '\n' 
        ||  'select ' 
        ||  '   -1 :: NUMBER(38, 0)                                                                             as ENCOUNTER_NUM, ' || '\n'
        ||  '   PATID :: NUMBER(38, 0)                                                                          as PATIENT_NUM, ' || '\n'
        ||  '   concat(''NAACCR|'', split_part(concept, ''_N'', -1), '':'', coalesce(concept_cd, ''''))         as CONCEPT_CD, ' || '\n'
        ||  '   ''@''                                                                                           as PROVIDER_ID, ' || '\n'
        ||  '   DATE_CASE_INITIATED_N2085 :: TIMESTAMP                                                          as START_DATE, ' || '\n'
        ||  '   ''@''                                                                                           as MODIFIER_CD, ' || '\n'
        ||  '   1                                                                                               as INSTANCE_NUM, ' || '\n'
        ||  '   ''''                                                                                            as VALTYPE_CD,' || '\n'
        ||  '   ''''                                                                                            as TVAL_CHAR, ' || '\n'
        ||  '   cast(null as  integer)                                                                          as NVAL_NUM, ' || '\n'
        ||  '   ''''                                                                                            as VALUEFLAG_CD,' || '\n'
        ||  '   cast(null as  integer)                                                                          as QUANTITY_NUM, ' || '\n'
        ||  '   ''@''                                                                                           as UNITS_CD, ' || '\n'
        ||  '   cast(DATE_CASE_COMPLETED_N2090 as TIMESTAMP)                                                    as END_DATE, ' || '\n'
        ||  '   ''@''                                                                                           as LOCATION_CD, ' || '\n'
        ||  '   cast(null as  text)                                                                             as OBSERVATION_BLOB, ' || '\n'
        ||  '   cast(null as  integer)                                                                          as CONFIDENCE_NUM, ' || '\n'
        ||  '   CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,' || '\n'
        ||  '   CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,' || '\n'
        ||  '   CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,' || '\n'
        ||  '   cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,   '     || '\n'                                                             
        ||  '   cast(null as  integer)                                                                          as UPLOAD_ID ' || '\n'
        ||  'from DEIDENTIFIED_PCORNET_CDM.CDM.DEID_TUMOR' || '\n'
        ||  'unpivot (concept_cd for concept IN (' || :column_names || '))' || '\n'
        ||  'order by patid;'
    ;
    execute immediate :v_sql;
    return v_sql;
END;

call create_tumor_fact();