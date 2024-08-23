CREATE OR REPLACE TABLE {metadata_schema}.ACS (
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

insert into {metadata_schema}.ACS (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
        C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
        M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
with distinct_trim_universe as (
   select  distinct RAW_OBSGEN_TYPE, RAW_OBSGEN_NAME,  trim(split_part(universe, 'Universe:', 2)) as universe, subject_area, DESC_1, DESC_2 from {source_schema}.DEID_OBS_GEN_SDOH_ACS
   order by universe,subject_area, DESC_1, DESC_2
)
select 
    1 c_hlevel
    ,  '\\SDOH\\ACS\\' C_FULLNAME
    , 'ACS' C_NAME
    , 'N' C_SYNONYM_CD
    , 'FA' C_VISUALATTRIBUTES
    , NULL C_TOTALNUM
    , NULL C_BASECODE
    , NULL C_METADATAXML
    , 'acs_fact.concept_cd' C_FACTTABLECOLUMN
    , 'concept_dimension' C_TABLENAME
    , 'concept_path' C_COLUMNNAME
    , 'T' C_COLUMNDATATYPE
    , 'LIKE' C_OPERATOR
    ,  '\\SDOH\\ACS\\' C_DIMCODE
    , NULL C_TOOLTIP
    , 'ACS' C_COMMENT
    , '@' M_APPLIED_PATH
    , CURRENT_DATE UPDATE_DATE
    , CURRENT_DATE DOWNLOAD_DATE
    , CURRENT_DATE IMPORT_DATE
    , 'MU' SOURCESYSTEM_CD
    , NULL VALUETYPE_CD
    , NULL M_EXCLUSION_CD
    , NULL C_PATH
    , NULL C_SYMBOL
union all
select distinct
    2 c_hlevel
    ,  '\\SDOH\\ACS\\' ||  universe || '\\'
    , universe
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'acs_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\SDOH\\ACS\\' || universe || '\\'
    , NULL
    , universe
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from  distinct_trim_universe
    union all
select distinct
    3 c_hlevel
    ,  '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\'
    , subject_area
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'acs_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\'
    , NULL
    , subject_area
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from  distinct_trim_universe
     union all
select distinct
    4 c_hlevel
    ,  '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\' || desc_1 || '\\'
    , desc_1
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'acs_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\' || desc_1 || '\\'
    , NULL
    , desc_1
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from  distinct_trim_universe
    union all
select distinct
    5 c_hlevel
    ,  '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\' || desc_1 || '\\' || desc_2 || '\\'
    , desc_2
    , 'N'
    , 'LA'
    , NULL
    , concat(raw_obsgen_type,':',raw_obsgen_name)
    , NULL
    , 'acs_fact.concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    , '\\SDOH\\ACS\\' ||  universe || '\\' || subject_area || '\\' || desc_1 || '\\' || desc_2 || '\\'
    , NULL
    , desc_2
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
    from  distinct_trim_universe order by c_hlevel;


update {metadata_schema}.acs
set c_fullname = replace(c_fullname, '\'', '');

update {metadata_schema}.acs
set c_dimcode = replace(c_dimcode,'\'', '');


DELETE FROM {metadata_schema}.TABLE_ACCESS
WHERE C_TABLE_CD = 'ACS';

Insert into {metadata_schema}.TABLE_ACCESS (C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,
    C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,
    C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD)
select
    'ACS'
    , 'ACS'
    , 'N'
    , 1
    , '\\SDOH\\ACS\\'
    , 'ACS'
    , 'N'
    , 'FA '
    , null
    , null
    , 'acs_fact.concept_cd'
    , 'concept_dimension'
    , 'concept_path'
    , 'T'
    , 'LIKE'
    , '\\SDOH\\ACS\\'
    , 'ACS'
    , null
    , null
    , null
    , null
;

delete from  {target_schema}.concept_dimension
where concept_path like '%\\SDOH\\ACS\\%';

insert into {target_schema}.concept_dimension (CONCEPT_PATH, CONCEPT_CD, NAME_CHAR, CONCEPT_BLOB, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID)
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
FROM {metadata_schema}.ACS
where c_basecode is  not null;