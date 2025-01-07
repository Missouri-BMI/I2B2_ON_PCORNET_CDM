create or replace table {metadata_schema}.ACT_MED_LIST_VA_V41 as
select * from {metadata_schema}.ACT_MED_VA_V41;

-- 
update {metadata_schema}.ACT_MED_LIST_VA_V41
set c_fullname = replace(c_fullname, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_dimcode = replace(c_dimcode, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_totalnum = null 
, c_facttablecolumn = 'med_list_fact.concept_cd';

-- concept_path
delete from {target_schema}.concept_dimension
where concept_path like '\\ACT\\HomeMedications\\%';

insert into {target_schema}.concept_dimension
with all_med as (
    select * from {target_schema}.concept_dimension
    where concept_path like '\\ACT\\Medications\\MedicationsByVaClass\\%'
)
select 
    replace(concept_path, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\') concept_path
    , concept_cd
    , name_char
    , concept_blob
    , update_date
    , download_date
    , import_date
    , sourcesystem_cd
    , upload_id
from all_med;


-- table access
delete from {metadata_schema}.table_access
where C_TABLE_CD = 'ACT_MED_LIST_VA_2018';

insert into {metadata_schema}.table_access
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
from {metadata_schema}.table_access 
where c_table_cd = 'ACT_MED_VA_2018';

update {metadata_schema}.table_access
set c_fullname = replace(c_fullname, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_dimcode = replace(c_dimcode, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_totalnum = null 
where C_TABLE_CD = 'ACT_MED_LIST_VA_2018';