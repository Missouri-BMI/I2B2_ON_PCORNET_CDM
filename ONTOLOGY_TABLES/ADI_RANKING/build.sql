USE DATABASE i2b2_dev;

USE SCHEMA i2b2metadata;
-- CREATE ONTOLOGY TABLE 
CREATE OR REPLACE TABLE ADI_RANKING (
        C_HLEVEL                                INT    NOT NULL,
        C_FULLNAME                              VARCHAR(700)   NOT NULL,
        C_NAME                                  VARCHAR(2000)  NOT NULL,
        C_SYNONYM_CD                            CHAR(1) NOT NULL,
        C_VISUALATTRIBUTES                      CHAR(3) NOT NULL,
        C_TOTALNUM                              INT    NULL,
        C_BASECODE                              VARCHAR(50)    NULL,
        C_METADATAXML                           TEXT    NULL,
        C_FACTTABLECOLUMN                       VARCHAR(50)    NOT NULL,
        C_TABLENAME                             VARCHAR(50)    NOT NULL,
        C_COLUMNNAME                            VARCHAR(50)    NOT NULL,
        C_COLUMNDATATYPE                        VARCHAR(50)    NOT NULL,
        C_OPERATOR                              VARCHAR(10)    NOT NULL,
        C_DIMCODE                               VARCHAR(700)   NOT NULL,
        C_COMMENT                               TEXT    NULL,
        C_TOOLTIP                               VARCHAR(900)   NULL,
        M_APPLIED_PATH                          VARCHAR(700)   NOT NULL,
        UPDATE_DATE                             DATE    NOT NULL,
        DOWNLOAD_DATE                           DATE    NULL,
        IMPORT_DATE                             DATE    NULL,
        SOURCESYSTEM_CD                         VARCHAR(50)    NULL,
        VALUETYPE_CD                            VARCHAR(50)    NULL,
        M_EXCLUSION_CD                          VARCHAR(25) NULL,
        C_PATH                                  VARCHAR(700)   NULL,
        C_SYMBOL                                VARCHAR(50)    NULL
);

-- Insert root 
insert into ADI_RANKING (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
        C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
        M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
with state_rank as (
    select distinct ADI_STATERNK as ADI_STATERNK from DEIDENTIFIED_PCORNET_CDM.CDM.DEID_ADI
    where adi_staternk is not null
    order by ADI_STATERNK
), 
national_rank as (
    select distinct ADI_NATRANK as ADI_NATRANK from DEIDENTIFIED_PCORNET_CDM.CDM.DEID_ADI
    where ADI_NATRANK is not null
    order by ADI_NATRANK
)
select 
    1
    ,  '\\ADI_RANKING\\'
    , 'ADI Ranks'
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    , 'concept_path'
    , 'T'
    , 'LIKE'
    ,  '\\ADI_RANKING\\'
    , NULL
    , 'ADI Ranks'
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
union all
-- Insert national rank root 
select
    2
    ,  '\\ADI_RANKING\\ADI_NATRANK\\'
    , 'ADI National Ranks'
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    ,  '\\ADI_RANKING\\ADI_NATRANK\\'
    , NULL
    , 'ADI National Ranks'
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
union all
-- Insert state rank root 
select
    2
    ,  '\\ADI_RANKING\\ADI_STATERNK\\'
    , 'ADI State Ranks'
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\ADI_RANKING\\ADI_STATERNK\\'
    , NULL
    , 'ADI State Ranks'
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
union all 
-- Insert state ranks 
select
    3
    ,  '\\ADI_RANKING\\ADI_STATERNK\\' || ADI_STATERNK || '\\'
    , 'ADI State Rank:' || ADI_STATERNK
    , 'N'
    , 'LA'
    , NULL
    , 'ADI_STATERNK:' || ADI_STATERNK
    , NULL
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\ADI_RANKING\\ADI_STATERNK\\' || ADI_STATERNK || '\\'
    , NULL
    , 'ADI State Rank:' || ADI_STATERNK
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from state_rank
union all 
-- Insert national ranks 
select
    3
    , '\\ADI_RANKING\\ADI_NATRANK\\' || ADI_NATRANK || '\\'
    , 'ADI National Rank:' || ADI_NATRANK
    , 'N'
    , 'LA'
    , NULL
    , 'ADI_NATRANK:' || ADI_NATRANK
    , NULL
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\ADI_RANKING\\ADI_NATRANK\\' || ADI_NATRANK || '\\'
    , NULL
    , 'ADI National Rank:' || ADI_NATRANK
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from national_rank;

-- Insert tables access ranks 
DELETE FROM TABLE_ACCESS
WHERE C_TABLE_CD = 'ADI_RANKING';

Insert into TABLE_ACCESS (C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,
    C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,
    C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD)
select
    'ADI_RANKING'
    , 'ADI_RANKING'
    , 'N'
    , 1
    , '\\ADI_RANKING\\'
    , 'ADI Ranks'
    , 'N'
    , 'FA '
    , null
    , null
    , 'adi_fact.concept_cd'
    , 'concept_dimension'
    , 'concept_path'
    , 'T'
    , 'LIKE'
    , '\\ADI_RANKING\\'
    , 'ADI Ranks'
    , null
    , null
    , null
    , null
;

delete from i2b2data.concept_dimension
where concept_path like '%\\ADI_RANKING\\%';

insert into i2b2data.concept_dimension (CONCEPT_PATH, CONCEPT_CD, NAME_CHAR, CONCEPT_BLOB, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID)
select 
    c_fullname
    , C_BASECODE
    , C_NAME
    , NULL
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
FROM ADI_RANKING
where c_basecode is  not null;