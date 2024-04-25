-- CREATE ONTLOGY CONCEPTS WITH PATH
CREATE OR REPLACE PROCEDURE generate_ontology_concepts_with_path()
RETURNS INTEGER NULL
LANGUAGE SQL
AS
DECLARE
    ont_tables RESULTSET DEFAULT (select c_table_name from i2b2metadata.table_access where c_visualattributes like '%A%');
    v_sqlStr TEXT DEFAULT '';
    ont_cur CURSOR for ont_tables;
BEGIN
   
    for r in ont_cur DO
         if (v_sqlStr != '') then
            v_sqlStr := v_sqlStr || '\nunion\n';
         end if;
         v_sqlStr := v_sqlStr || 'SELECT C_FULLNAME, C_FACTTABLECOLUMN FROM i2b2metadata.' || r.c_table_name || '\n where lower(C_FACTTABLECOLUMN) like ''%concept_cd%'' and                     lower(C_COLUMNNAME) = ''concept_path''';
    END FOR;
    
    -- create concepts with paths
    execute immediate ('create or replace TEMP TABLE ONT_CONCEPTS_WITH_PATH as \n' || v_sqlStr);
END;