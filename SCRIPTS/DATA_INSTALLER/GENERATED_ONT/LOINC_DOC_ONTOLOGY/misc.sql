use database i2b2_prod;
use schema i2b2metadata;

create or replace table Document_Ontology
as 
select * from i2b2_dev.i2b2metadata.document_ontology;

insert into table_access 
select * from i2b2_dev.i2b2metadata.table_access
where c_table_name = 'Document_Ontology';

insert into i2b2data.concept_dimension 
select * from i2b2_dev.i2b2data.concept_dimension
where concept_path like '%\\DocumentOntology\\%';

update table_access
set C_TOTALNUM=null
where c_fullname like '%\\DocumentOntology\\%';

update document_ontology
set C_TOTALNUM=null;