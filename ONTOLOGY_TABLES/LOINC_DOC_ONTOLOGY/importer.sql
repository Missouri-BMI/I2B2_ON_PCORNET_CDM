DELETE FROM TABLE_ACCESS
WHERE C_TABLE_NAME = 'Document_Ontology';

Insert into TABLE_ACCESS (C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,
    C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,
    C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD)
select
    'Document_Ontology'
    , 'Document_Ontology'
    , 'N'
    , 1
    , '\\DocumentOntology\\'
    , 'Document Ontology'
    , 'N'
    , 'FA '
    , null
    , null
    , 'concept_cd'
    , 'concept_dimension'
    , 'concept_path'
    , 'T'
    , 'LIKE'
    , '\\DocumentOntology\\'
    , 'Document Ontology'
    , null
    , null
    , null
    , null
;


insert into Document_Ontology (C_HLEVEL, C_FULLNAME, C_NAME, C_SYNONYM_CD, C_VISUALATTRIBUTES, C_TOTALNUM, C_BASECODE,
        C_METADATAXML, C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_COLUMNDATATYPE, C_OPERATOR, C_DIMCODE, C_COMMENT, C_TOOLTIP,
        M_APPLIED_PATH, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, VALUETYPE_CD, M_EXCLUSION_CD, C_PATH, C_SYMBOL)
select 
    1
    ,  '\\DocumentOntology\\'
    , 'Document Ontology'
    , 'N'
    , 'FA'
    , NULL
    , NULL
    , NULL
    , 'concept_cd'
    , 'concept_dimension'
    ,  'concept_path'
    , 'T'
    , 'LIKE'
    ,  '\\DocumentOntology\\'
    , NULL
    , 'Document Ontology'
    , '@'
    , CURRENT_DATE
    , CURRENT_DATE
    , CURRENT_DATE
    , 'MU'
    , NULL
    , NULL
    , NULL
    , NULL
;


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
FROM Document_Ontology
where c_basecode is  not null;