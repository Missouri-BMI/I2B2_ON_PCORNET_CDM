DELETE FROM I2B2METADATA.TABLE_ACCESS
WHERE C_TABLE_NAME = 'NAACCR_ONTOLOGY';

Insert into I2B2METADATA.TABLE_ACCESS (C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,
    C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,
    C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD)
select
    'NAACCR_ONTOLOGY'
    , 'NAACCR_ONTOLOGY'
    , 'N'
    , c_hlevel
    , C_FULLNAME
    , C_NAME
    , C_SYNONYM_CD
    , C_VISUALATTRIBUTES
    , null
    , C_BASECODE
    , C_FACTTABLECOLUMN
    , 'CONCEPT_DIMENSION'
    , C_COLUMNNAME
    , C_COLUMNDATATYPE
    , C_OPERATOR
    , C_DIMCODE
    , C_TOOLTIP
    , null
    , null
    , null
    , null
from I2B2METADATA.NAACCR_ONTOLOGY
where c_hlevel = 1
;


update i2b2metadata.table_access
        set c_facttablecolumn = 'tumor_fact.concept_cd'
        where c_table_name = 'NAACCR_ONTOLOGY';

update i2b2metadata.NAACCR_ONTOLOGY
set c_facttablecolumn = 'tumor_fact.concept_cd';


delete from i2b2data.concept_dimension
where concept_path like '\\i2b2\\naaccr\\%';

insert into i2b2data.concept_dimension (CONCEPT_PATH, CONCEPT_CD, NAME_CHAR, CONCEPT_BLOB, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID)
    select 
        C_FULLNAME
        , C_BASECODE
        , C_BASECODE
        , NULL
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , NULL
        , NULL
    FROM i2b2metadata.NAACCR_ONTOLOGY
    WHERE C_BASECODE IS NOT NULL;
    
call i2b2metadata.RUNTOTALNUM('TUMOR_FACT', 'I2B2DATA', 'NAACCR_ONTOLOGY');

