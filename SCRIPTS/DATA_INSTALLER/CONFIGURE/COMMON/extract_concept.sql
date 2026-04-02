CREATE OR REPLACE FILE FORMAT {{ stage_schema }}.PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

CREATE OR REPLACE STAGE {{ stage_schema }}.PARQUET_STAGE_CONCEPT
    FILE_FORMAT = {{ stage_schema }}.PARQUET_FORMAT;
    
CREATE OR REPLACE STAGE {{ stage_schema }}.PARQUET_STAGE_ONT
    FILE_FORMAT = {{ stage_schema }}.PARQUET_FORMAT;

CREATE OR REPLACE PROCEDURE {{ stage_schema }}.generate_copy_statements()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    table_name STRING;
    results RESULTSET;
    query TEXT;
BEGIN
    let table_schema := split_part('{{ stage_schema }}', '.', 2);
    query := 'SELECT table_name FROM information_schema.tables WHERE lower(table_schema) = ''' || table_schema || '''';
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    
    FOR record IN cur DO
        table_name := record.table_name;
        
        IF (table_name = 'CONCEPT_DIMENSION') THEN
            query := 'COPY INTO @{{ stage_schema }}.PARQUET_STAGE_CONCEPT/' || table_name || '.parquet ' ||
                 'FROM {{ stage_schema }}.' || table_name || ' ' ||
                 'OVERWRITE = TRUE ' ||
                 'MAX_FILE_SIZE = 67108864 ' || -- 64 MB
                 'HEADER = TRUE ' ||
                 'FILE_FORMAT = (FORMAT_NAME = ''{{ stage_schema }}.PARQUET_FORMAT'')'
                 ; 
            EXECUTE IMMEDIATE :query;     
        ELSE
            query := 'COPY INTO @{{ stage_schema }}.PARQUET_STAGE_ONT/' || table_name || '.parquet ' ||
                 'FROM {{ stage_schema }}.' || table_name || ' ' ||
                 'OVERWRITE = TRUE ' ||
                 'MAX_FILE_SIZE = 67108864 ' || -- 64 MB
                 'HEADER = TRUE ' ||
                 'FILE_FORMAT = (FORMAT_NAME = ''{{ stage_schema }}.PARQUET_FORMAT'')'
                 ; 
            EXECUTE IMMEDIATE :query;
        END IF;
    END FOR;

    RETURN 'Successfully generated and executed copy statements to Parquet stages.';
END;
$$;

-- Export all tables in the stage schema to Parquet
CALL {{ stage_schema }}.generate_copy_statements();