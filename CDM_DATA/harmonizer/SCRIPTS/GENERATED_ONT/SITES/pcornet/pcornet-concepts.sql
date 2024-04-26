--clean up
delete from ACT_DEM_V41
where c_fullname like '%\\ACT\\Demographics\\PCORNet Sites\\%';

--load csv
delete from concept_dimension
where concept_path like '%\\ACT\\Demographics\\PCORNet Sites\\%';

insert into concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
        select
            c_dimcode
            , c_basecode
            , c_name
            , null
            , current_timestamp
            , current_timestamp
            , current_timestamp
            , 'ACT'
            , null
        from ACT_DEM_V41
        where c_fullname like  '%\\ACT\\Demographics\\PCORNet Sites\\%' and c_basecode is not null;