update act_sdoh_v4
set c_facttablecolumn = 'sdoh_fact.concept_cd';

update table_access
set c_facttablecolumn = 'sdoh_fact.concept_cd' where c_table_name = 'ACT_SDOH_V4';