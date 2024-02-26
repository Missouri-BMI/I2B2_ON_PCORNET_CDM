-- visit type

update act_visit_details_v4 
set 
c_basecode = concat('VISIT|TYPE:', c_basecode)
, c_facttablecolumn = 'visit_fact.concept_cd'
, c_tablename = 'concept_dimension'
, c_columnname = 'concept_path'
, c_columndatatype = 'T'
, c_operator = 'LIKE'
, c_dimcode = c_fullname
where c_fullname like '%\\ACT\\Visit Details\\Visit type\\%';

insert into i2b2data.concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
select c_fullname
, c_basecode
, c_name
, null
, current_timestamp
, current_timestamp
, current_timestamp
, 'ACT'
, null
from act_visit_details_v4
where c_fullname like  '%\\ACT\\Visit Details\\Visit type\\%' and c_basecode is not null; 

-- lenght of stay

update act_visit_details_v4 
set 
c_basecode = concat('VISIT|LENGTH:', c_basecode)
, c_facttablecolumn = 'visit_fact.concept_cd'
, c_tablename = 'concept_dimension'
, c_columnname = 'concept_path'
, c_columndatatype = 'T'
, c_operator = 'LIKE'
, c_dimcode = c_fullname
where c_fullname like '%\\ACT\\Visit Details\\Length of stay\\%';



insert into i2b2data.concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
select c_fullname
, c_basecode
, c_name
, null
, current_timestamp
, current_timestamp
, current_timestamp
, 'ACT'
, null
from act_visit_details_v4
where c_fullname like '%\\ACT\\Visit Details\\Length of stay\\%' and c_basecode is not null; 