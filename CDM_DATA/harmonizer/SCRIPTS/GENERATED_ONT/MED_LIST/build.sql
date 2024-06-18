use database i2b2_dev;
use schema i2b2metadata;
create or replace table ACT_MED_LIST_VA_V41 as
select * from ACT_MED_VA_V41;

update ACT_MED_LIST_VA_V41
set c_totalnum = null;

insert into table_access
select 
    'ACT_MED_LIST_VA_2018' as C_TABLE_CD,
    'ACT_MED_LIST_VA_V41' as C_TABLE_NAME,
    C_PROTECTED_ACCESS,
    C_ONTOLOGY_PROTECTION,
    C_HLEVEL,
    C_FULLNAME,
    'ACT Home Medications VA Classes' C_NAME,
    C_SYNONYM_CD,
    c_visualattributes,
    null as c_totalnum,
    c_basecode,
    c_metadataxml,
    'med_list_fact.concept_cd' as c_facttablecolumn,
    c_dimtablename,
    c_columnname,
    c_columndatatype,
    c_operator,
    c_dimcode,
    c_comment,
    'ACT Home Medications' as c_tooltip,
    c_entry_date,
    c_change_date,
    c_status_cd,
    valuetype_cd
from table_access 
where c_table_cd = 'ACT_MED_VA_2018';