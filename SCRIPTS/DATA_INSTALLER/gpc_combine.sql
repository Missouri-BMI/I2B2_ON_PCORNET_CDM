CREATE OR REPLACE PROCEDURE CDM_DATALAKE.PUBLIC.RUN_GPC_ETL_PROCESS(
    SOURCE_DB_NAME STRING,       -- 'DEIDENTIFIED_GROUSE_DB'
    SOURCE_SCHEMA_PREFIX STRING, -- 'PCORNET_CDM_'
    SOURCE_TABLE_PREFIX STRING,  -- 'V_'
    TARGET_TABLE_PREFIX STRING,  -- 'GPC_'
    REFERENCE_SCHEMA STRING,     -- 'PCORNET_CDM_MU'
    TARGET_DB_NAME STRING,       -- 'CDM_DATALAKE'
    TARGET_SCHEMA_NAME STRING,   -- 'GPC'
    SITE_DATAMARTID_FILTER STRING -- NULL = all sites, 'C4WU' = WashU only
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_site_schema STRING;
    v_datamart_id STRING;

    v_datamart_query STRING;
    v_insert_sql STRING;

    v_source_table_name STRING;
    v_target_view_name STRING;
    v_union_query STRING;
    v_create_view_sql STRING;

    meta_query STRING;

    rs_sites RESULTSET;
    rs_datamart RESULTSET;
    rs_view_definitions RESULTSET;

BEGIN
    -- ----------------------------
    -- Phase 0: reset target schema
    -- ----------------------------
    EXECUTE IMMEDIATE 'CREATE OR REPLACE SCHEMA ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME;

    EXECUTE IMMEDIATE 'CREATE OR REPLACE SEQUENCE ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.PATIENT_SEQ START = 1 INCREMENT = 1';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE SEQUENCE ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.ENCOUNTER_SEQ START = 1 INCREMENT = 1';

    EXECUTE IMMEDIATE '
      CREATE OR REPLACE TABLE ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.PATIENT_CROSSWALK (
        patient_num NUMBER(38,0) DEFAULT ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.PATIENT_SEQ.NEXTVAL,
        patid VARCHAR(701) NOT NULL,
        pcornet_site_name VARCHAR(100) NOT NULL,
        PRIMARY KEY (patid, pcornet_site_name)
      )
    ';

    EXECUTE IMMEDIATE '
      CREATE OR REPLACE TABLE ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.ENCOUNTER_CROSSWALK (
        encounter_num NUMBER(38,0) DEFAULT ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.ENCOUNTER_SEQ.NEXTVAL,
        encounterid VARCHAR(701) NOT NULL,
        pcornet_site_name VARCHAR(100) NOT NULL,
        PRIMARY KEY (encounterid, pcornet_site_name)
      )
    ';

    -- ✅ Temp table of eligible schemas based on DATAMARTID
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMP TABLE SITE_FILTER_SCHEMAS (TABLE_SCHEMA STRING, DATAMARTID STRING)';

    -- ---------------------------------------
    -- Phase 1: discover schemas + crosswalks
    -- ---------------------------------------
    rs_sites := (EXECUTE IMMEDIATE '
        SELECT DISTINCT TABLE_SCHEMA
        FROM ' || SOURCE_DB_NAME || '.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA LIKE ''' || SOURCE_SCHEMA_PREFIX || '%''
          AND TABLE_NAME = ''V_DEID_HARVEST''
    ');

    FOR site_rec IN rs_sites DO
        v_site_schema := site_rec.TABLE_SCHEMA;

        v_datamart_query :=
          'SELECT MAX(DATAMARTID) AS DM_ID FROM ' || SOURCE_DB_NAME || '.' || v_site_schema || '.V_DEID_HARVEST';

        rs_datamart := (EXECUTE IMMEDIATE :v_datamart_query);

        v_datamart_id := NULL;
        FOR dm_row IN rs_datamart DO
            v_datamart_id := dm_row.DM_ID;
        END FOR;

        IF (v_datamart_id IS NULL) THEN
            CONTINUE;
        END IF;

        -- ✅ filter by DATAMARTID ONLY
        IF (SITE_DATAMARTID_FILTER IS NOT NULL AND v_datamart_id <> SITE_DATAMARTID_FILTER) THEN
            CONTINUE;
        END IF;

        -- save eligible schema for Phase 2
        EXECUTE IMMEDIATE
          'INSERT INTO SITE_FILTER_SCHEMAS(TABLE_SCHEMA, DATAMARTID)
           VALUES (''' || v_site_schema || ''',''' || v_datamart_id || ''')';

        -- PATIENT crosswalk
        v_insert_sql := '
          INSERT INTO ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.PATIENT_CROSSWALK (patid, pcornet_site_name)
          SELECT DISTINCT d.PATID, ''' || v_datamart_id || '''
          FROM ' || SOURCE_DB_NAME || '.' || v_site_schema || '.V_DEID_DEMOGRAPHIC d
          WHERE d.PATID IS NOT NULL
        ';
        EXECUTE IMMEDIATE v_insert_sql;

        -- ENCOUNTER crosswalk
        v_insert_sql := '
          INSERT INTO ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.ENCOUNTER_CROSSWALK (encounterid, pcornet_site_name)
          SELECT DISTINCT e.ENCOUNTERID, ''' || v_datamart_id || '''
          FROM ' || SOURCE_DB_NAME || '.' || v_site_schema || '.V_DEID_ENCOUNTER e
          WHERE e.ENCOUNTERID IS NOT NULL
        ';
        EXECUTE IMMEDIATE v_insert_sql;

    END FOR;

    -- ---------------------------------------
    -- Phase 2: create consolidated UNION views
    -- (restricted to SITE_FILTER_SCHEMAS)
    -- ---------------------------------------
    meta_query := '
      WITH REFERENCE_METADATA AS (
        SELECT
          TABLE_NAME,
          LISTAGG(''t.'' || COLUMN_NAME, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS COL_LIST,
          MAX(IFF(COLUMN_NAME = ''PATID'', 1, 0)) AS HAS_PATID,
          MAX(IFF(COLUMN_NAME = ''ENCOUNTERID'', 1, 0)) AS HAS_ENCOUNTERID
        FROM ' || SOURCE_DB_NAME || '.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''' || REFERENCE_SCHEMA || '''
        GROUP BY TABLE_NAME
      )
      SELECT
        v.TABLE_NAME,
        LISTAGG(
          ''SELECT '' || rm.COL_LIST ||

          CASE WHEN UPPER(v.TABLE_NAME) LIKE ''%DEMOGRAPHIC'' THEN
            '', (SELECT MAX(DATAMARTID) FROM ' || SOURCE_DB_NAME || '.'' || v.TABLE_SCHEMA || ''.V_DEID_HARVEST) AS PCORNET_SITE_ID'' ||
            '', (SELECT MAX(DATAMART_NAME) FROM ' || SOURCE_DB_NAME || '.'' || v.TABLE_SCHEMA || ''.V_DEID_HARVEST) AS PCORNET_SITE_NAME''
          ELSE '''' END ||

          CASE WHEN rm.HAS_PATID = 1 THEN '', pc.patient_num'' ELSE '''' END ||
          CASE WHEN rm.HAS_ENCOUNTERID = 1 THEN '', ec.encounter_num'' ELSE '''' END ||
          '' '' || CHAR(10) ||

          ''FROM ' || SOURCE_DB_NAME || '.'' || v.TABLE_SCHEMA || ''.'' || v.TABLE_NAME || '' t '' || CHAR(10) ||

          CASE WHEN rm.HAS_PATID = 1 THEN
            ''LEFT JOIN ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.PATIENT_CROSSWALK pc '' ||
            ''ON t.PATID = pc.patid AND pc.pcornet_site_name = (SELECT MAX(DATAMARTID) FROM ' || SOURCE_DB_NAME || '.'' || v.TABLE_SCHEMA || ''.V_DEID_HARVEST) '' || CHAR(10)
          ELSE '''' END ||

          CASE WHEN rm.HAS_ENCOUNTERID = 1 THEN
            ''LEFT JOIN ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.ENCOUNTER_CROSSWALK ec '' ||
            ''ON t.ENCOUNTERID = ec.encounterid AND ec.pcornet_site_name = (SELECT MAX(DATAMARTID) FROM ' || SOURCE_DB_NAME || '.'' || v.TABLE_SCHEMA || ''.V_DEID_HARVEST) '' || CHAR(10)
          ELSE '''' END ||

          CHAR(10),
          '' UNION ALL ''
        ) WITHIN GROUP (ORDER BY v.TABLE_SCHEMA) AS UNION_QUERY
      FROM ' || SOURCE_DB_NAME || '.INFORMATION_SCHEMA.VIEWS v
      JOIN REFERENCE_METADATA rm ON v.TABLE_NAME = rm.TABLE_NAME
      JOIN SITE_FILTER_SCHEMAS sfs ON sfs.TABLE_SCHEMA = v.TABLE_SCHEMA
      WHERE STARTSWITH(v.TABLE_NAME, ''' || SOURCE_TABLE_PREFIX || ''')
        AND STARTSWITH(v.TABLE_SCHEMA, ''' || SOURCE_SCHEMA_PREFIX || ''')
      GROUP BY v.TABLE_NAME
    ';

    rs_view_definitions := (EXECUTE IMMEDIATE :meta_query);

    FOR rec IN rs_view_definitions DO
        v_source_table_name := rec.TABLE_NAME;
        v_union_query := rec.UNION_QUERY;

        v_target_view_name := TARGET_TABLE_PREFIX || SUBSTR(v_source_table_name, LENGTH(SOURCE_TABLE_PREFIX) + 1);

        v_create_view_sql :=
          'CREATE OR REPLACE VIEW ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME || '.' || v_target_view_name ||
          ' AS ' || CHAR(10) || v_union_query;

        EXECUTE IMMEDIATE v_create_view_sql;
    END FOR;

    RETURN 'Success: built ' || TARGET_DB_NAME || '.' || TARGET_SCHEMA_NAME ||
           ' sites=' || (SELECT COUNT(*)::STRING FROM SITE_FILTER_SCHEMAS) ||
           CASE WHEN SITE_DATAMARTID_FILTER IS NULL THEN ' (all)' ELSE ' (DATAMARTID=' || SITE_DATAMARTID_FILTER || ')' END;

END;
$$;



--------------------------------------------------------------------------------
-- TASK EXAMPLE: ALL SITES
--------------------------------------------------------------------------------
CREATE OR REPLACE TASK CDM_DATALAKE.PUBLIC.TASK_REFRESH_GPC
    WAREHOUSE = 'I2B2_ETL_WH'
    SCHEDULE = 'USING CRON 0 2 8 * * UTC'
AS
    CALL CDM_DATALAKE.PUBLIC.RUN_GPC_ETL_PROCESS(
        'DEIDENTIFIED_GROUSE_DB',
        'PCORNET_CDM_',
        'V_',
        'GPC_',
        'PCORNET_CDM_MU',
        'CDM_DATALAKE',
        'GPC',
        NULL   -- SITE_DATAMARTID_FILTER
    );

--------------------------------------------------------------------------------
-- TASK EXAMPLE: WASHU ONLY (DATAMARTID = C4WU)
--------------------------------------------------------------------------------
CREATE OR REPLACE TASK CDM_DATALAKE.PUBLIC.TASK_REFRESH_GPC_WASHU
    WAREHOUSE = 'I2B2_ETL_WH'
    SCHEDULE = 'USING CRON 0 3 8 * * UTC'
AS
    CALL CDM_DATALAKE.PUBLIC.RUN_GPC_ETL_PROCESS(
        'DEIDENTIFIED_GROUSE_DB',
        'PCORNET_CDM_',
        'V_',
        'GPC_',
        'PCORNET_CDM_MU',
        'CDM_DATALAKE',
        'WASHU',
        'C4WU' -- SITE_DATAMARTID_FILTER
    );

ALTER TASK CDM_DATALAKE.PUBLIC.TASK_REFRESH_GPC RESUME;
ALTER TASK CDM_DATALAKE.PUBLIC.TASK_REFRESH_GPC_WASHU RESUME;
