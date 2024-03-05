CREATE OR REPLACE PROCEDURE dem_age_parser(
segment VARCHAR, 
date_adjust BOOLEAN
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    new_dim_code VARCHAR DEFAULT '';
    segments ARRAY;
    left_sub_segment VARCHAR;
    new_sub_split VARCHAR;
    right_sub_segment VARCHAR;
    new_split ARRAY;
    temp VARCHAR;
    dateVal INT;
BEGIN
    segments :=  split(segment,'-');
    left_sub_segment = GET(segments, 0);
    --left sub_segment
    if (CONTAINS(left_sub_segment, 'NOW()')) then
        new_dim_code := 'CURRENT_DATE';
        if(date_adjust) then
            new_dim_code := concat(new_dim_code, ' + (INTERVAL \'1 day\')');
        end if;
    else
        return segment;--cannt parse
    end if;

    -- right sub_segment
    if (len(segments) = 2) then
        right_sub_segment := GET(segments, 1);
        new_split := split(right_sub_segment, '*');
        if (len(new_split) == 2) then
            new_sub_split := split(GET(new_split, 0), '/');
            if (len(new_sub_split) == 2):
                temp := REPLACE(REPLACE(GET(new_sub_split, 0), '(',''), ')','');
                dateVal := (temp :: INT) + 1;
                temp := (dateVal :: VARCHAR);
                new_dim_code := concat(new_dim_code, ' - (INTERVAL \'', temp, ' month\')');    
            else
                temp := REPLACE(REPLACE(GET(new_sub_split, 0), '(',''), ')','');
                dateVal := (temp :: INT);
                temp := (dateVal :: VARCHAR);
                new_dim_code := concat(new_dim_code, ' - (INTERVAL \'', temp, ' year\')');
            end if;
        else
            new_dim_code := concat(new_dim_code, ' - (INTERVAL \'1 year\')');
        end if;
    end if;

    return new_dim_code
END;
$$
;

CREATE OR REPLACE PROCEDURE dem_age_convert(

)
RETURNS VARCHAR NULL
LANGUAGE SQL
AS
$$
DECLARE
    results RESULTSET;
    query VARCHAR DEFAULT 'select C_FULLNAME, C_OPERATOR, C_DIMCODE from ACT_DEM_V41 where C_FULLNAME LIKE \'\\ACT\\Demographics\\Age\\%\' and C_OPERATOR != \'LIKE\'';
    dim_code VARCHAR;
    converted_dim_code VARCHAR;
    segments ARRAY;
    right_part VARCHAR;
    left_part VARCHAR;
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
        dim_code := record.C_DIMCODE;
        segments := SPLIT(dim_code, 'AND');
        
        if (len(segments) == 2) then
            left_part := GET(segments, 0);
            left_part := call dem_age_parser(:left_part, True);

            right_part := GET(segments, 1);
            right_part := call dem_age_parser(:right_part, False);
            
            converted_dim_code := concat(:left_part, ' AND ', :right_part);
        else if (len(segments) == 1) then
            left_part := GET(segments, 0);
            left_part := call dem_age_parser(left_part, False);
            converted_dim_code = call dem_age_parser(left_part, False);
        end if;
        
        query := 'update ACT_DEM_V41 set C_DIMCODE=\'' || converted_dim_code || '\'' || 'where c_fullname=' || record.c_fullname ||'\'';
        execute immediate :query;
    
    end for;
END;
$$
;

call dem_age_convert();