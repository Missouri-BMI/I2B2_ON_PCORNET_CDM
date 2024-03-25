CREATE OR REPLACE PROCEDURE RUN_ONT_SUMMARAY(

)
RETURNS TABLE()
LANGUAGE SQL
AS
DECLARE
    results RESULTSET;
    query VARCHAR DEFAULT 'select c_name, c_table_name from table_access where c_table_cd not in (\'MISCELLANEOUS_OBSERVATIONS\') and C_VISUALATTRIBUTES not like \'FH%\'';
    tableName VARCHAR;
  
    query_str VARCHAR DEFAULT '';
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
    
        query := 'select ' || '\'' || record.c_name || '\'' || ' as "Ontology Table", count(*) as "unique rows" ' || ', (select count(*) from ' || record.c_table_name || ' where C_VISUALATTRIBUTES like \'LA%\') as "leaf nodes" from ' || record.c_table_name; 
        query := query ;
        if (query_str = '') then
            query_str := query;
        else
            query_str := query_str || '\n\n UNION ALL \n\n' || query;
        end if;
    end for;
    query_str := query_str || ' \n\n ' || 'order by "unique rows" desc';
    results := (EXECUTE IMMEDIATE :query_str);
   return TABLE(results);
END;

CALL RUN_ONT_SUMMARAY();





