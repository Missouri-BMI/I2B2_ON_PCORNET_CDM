-- CRC SCHEMA IS HARD CODED TO i2b2data
-- ONT SCHEMA IS HARD CODED TO i2b2metadata
-- ONTOLOGY TABLE NAME IS HARD CODED TO 'MISCELLANEOUS_OBSERVATIONS' AND DISPLAY NAME TO 'MISCELLANEOUS OBSERVATIONS'
BEGIN
    use schema report;
    DROP TABLE IF EXISTS I2B2METADATA.GENERAL_OBSERVATIONS;
    CREATE OR REPLACE TABLE I2B2METADATA.MISCELLANEOUS_OBSERVATIONS (
            C_HLEVEL                              INT    NOT NULL,
            C_FULLNAME                    VARCHAR(700)   NOT NULL,
            C_NAME                                VARCHAR(2000)  NOT NULL,
            C_SYNONYM_CD                  CHAR(1) NOT NULL,
            C_VISUALATTRIBUTES    CHAR(3) NOT NULL,
            C_TOTALNUM                    INT    NULL,
            C_BASECODE                    VARCHAR(50)    NULL,
            C_METADATAXML                 TEXT    NULL,
            C_FACTTABLECOLUMN             VARCHAR(50)    NOT NULL,
            C_TABLENAME                   VARCHAR(50)    NOT NULL,
            C_COLUMNNAME                  VARCHAR(50)    NOT NULL,
            C_COLUMNDATATYPE              VARCHAR(50)    NOT NULL,
            C_OPERATOR                    VARCHAR(10)    NOT NULL,
            C_DIMCODE                             VARCHAR(700)   NOT NULL,
            C_COMMENT                             TEXT    NULL,
            C_TOOLTIP                             VARCHAR(900)   NULL,
            M_APPLIED_PATH                VARCHAR(700)   NOT NULL,
            UPDATE_DATE                   DATE    NOT NULL,
            DOWNLOAD_DATE                 DATE    NULL,
            IMPORT_DATE                   DATE    NULL,
            SOURCESYSTEM_CD               VARCHAR(50)    NULL,
            VALUETYPE_CD                  VARCHAR(50)    NULL,
            M_EXCLUSION_CD                        VARCHAR(25) NULL,
            C_PATH                                VARCHAR(700)   NULL,
            C_SYMBOL                              VARCHAR(50)    NULL
    )
    ;
-- ADD ENTRY IN TABLE ACCESS

    DELETE FROM I2B2METADATA.TABLE_ACCESS WHERE C_TABLE_NAME = 'GENERAL_OBSERVATIONS' or C_TABLE_NAME = 'MISCELLANEOUS_OBSERVATIONS';
    DELETE FROM I2B2DATA.concept_dimension WHERE CONCEPT_PATH LIKE '%GENERAL OBSERVATIONS%' or CONCEPT_PATH LIKE '%MISCELLANEOUS OBSERVATIONS%';

    Insert into I2B2METADATA.TABLE_ACCESS (C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,
        C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,
        C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD)
    with concepts as (
        select  distinct
            split_part(concept_path, '\\', 2) gn
        from not_mapped 
        order by gn
    )
    select
        'MISCELLANEOUS_OBSERVATIONS'
        , 'MISCELLANEOUS_OBSERVATIONS'
        , 'N'
        , 1
        , '\\' || gn || '\\'
        , 'Miscellaneous Observations'
        , 'N'
        , 'CA '
        , null
        , null
        , 'concept_cd'
        , 'concept_dimension'
        , 'concept_path'
        , 'T'
        , 'LIKE'
        , '\\' || gn || '\\'
        , gn
        , null
        , null
        , null
        , null
    from concepts
    ;


    -- ADD ROOT ENTRY IN ONT TABLE
    insert into i2b2metadata.MISCELLANEOUS_OBSERVATIONS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
            C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
            M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
    with concepts as (
        select  distinct
            split_part(concept_path, '\\', 2) gn
        from not_mapped 
        order by gn
    )    
    select 
        1
        , '\\' || gn || '\\'
        , 'Miscellaneous Observations'
        , 'N'
        , 'CA'
        , NULL
        , NULL
        , NULL
        , 'concept_cd'
        , 'concept_dimension'
        ,  'concept_path'
        , 'T'
        , 'LIKE'
        , '\\' || gn || '\\'
        , NULL
        , gn
        , '@'
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
        , NULL
        , NULL
        , NULL
    FROM concepts;


    -- ADD FACT CATEGORY IN ONT TABLE
    insert into i2b2metadata.MISCELLANEOUS_OBSERVATIONS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
            C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
            M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
    with concepts as (
        select  distinct
        split_part(concept_path, '\\', 2) gn
        , split_part(concept_path, '\\', 3) fact
        , fact_table
        from not_mapped order by gn,fact
    )    
    select 
        2
        , '\\' || gn || '\\' || fact || '\\'
        , fact
        , 'N'
        , 'CA'
        , NULL
        , NULL
        , NULL
        , fact_table || '.concept_cd'
        , 'concept_dimension'
        ,  'concept_path'
        , 'T'
        , 'LIKE'
        , '\\' || gn || '\\' || fact || '\\'
        , NULL
        , fact
        , '@'
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
        , NULL
        , NULL
        , NULL
    FROM concepts;



    -- ADD BASE CODE CATEGORY IN ONT TABLE
    insert into i2b2metadata.MISCELLANEOUS_OBSERVATIONS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
            C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
            M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
    with concepts as (
        select  distinct
            split_part(concept_path, '\\', 2) gn
            , split_part(concept_path, '\\', 3) fact
            , split_part(concept_path, '\\', 4) base
            , fact_table
        from not_mapped 
        order by gn,fact,base
    )    
    select 
        3
        , '\\' || gn || '\\' || fact || '\\' || base || '\\'
        , base
        , 'N'
        , 'CA'
        , NULL
        , NULL
        , NULL
        , fact_table || '.concept_cd'
        , 'concept_dimension'
        ,  'concept_path'
        , 'T'
        , 'LIKE'
        , '\\' || gn || '\\' || fact || '\\' || base || '\\'
        , NULL
        , base
        , '@'
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
        , NULL
        , NULL
        , NULL
    FROM concepts;


    -- ADD RANGE CATEGORY IN ONT TABLE
    insert into i2b2metadata.MISCELLANEOUS_OBSERVATIONS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
            C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
            M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
    with concepts as (
        select  distinct
            split_part(concept_path, '\\', 2) gn
            , split_part(concept_path, '\\', 3) fact
            , split_part(concept_path, '\\', 4) base
            , split_part(concept_path, '\\', 5) r
            , fact_table
            from not_mapped 
            order by gn,fact,base,r
    )    
    select 
        4
        , '\\' || gn || '\\' || fact || '\\' || base || '\\' || r || '\\'
        , r
        , 'N'
        , 'CA'
        , NULL
        , NULL
        , NULL
        , fact_table || '.concept_cd'
        , 'concept_dimension'
        ,  'concept_path'
        , 'T'
        , 'LIKE'
        , '\\' || gn || '\\' || fact || '\\' || base || '\\' || r || '\\'
        , NULL
        , r
        , '@'
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
        , NULL
        , NULL
        , NULL
    FROM concepts;


    -- ADD leaves concepts
    insert into i2b2metadata.MISCELLANEOUS_OBSERVATIONS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
            C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
            M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
    with concepts as (
        select  
            concept_path
            , concept_cd
            , fact_table
            , PATIENT_COUNT
            from not_mapped 
    )    
    select 
        5
        , concept_path
        , concept_cd
        , 'N'
        , 'LA'
        , PATIENT_COUNT
        , concept_cd
        , NULL
        , fact_table || '.concept_cd'
        , 'concept_dimension'
        ,  'concept_path'
        , 'T'
        , 'LIKE'
        , concept_path
        , NULL
        , concept_cd
        , '@'
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
        , NULL
        , NULL
        , NULL
    FROM concepts;


    --ADD concept_dimensions

    insert into i2b2data.concept_dimension (CONCEPT_PATH, CONCEPT_CD, NAME_CHAR, CONCEPT_BLOB, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID)
    select 
        concept_path
        , concept_cd
        , concept_cd
        , NULL
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , 'MU'
        , NULL
    FROM not_mapped;
END;