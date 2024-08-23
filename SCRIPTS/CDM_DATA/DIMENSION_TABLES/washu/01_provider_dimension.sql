-- create view
create or replace view {target_schema}.PROVIDER_DIMENSION
as 
select 
    PROVIDERID                                              as PROVIDER_ID,
    cast('@'  as VARCHAR(700))                              as PROVIDER_PATH,
    PROVIDER_SEX,
    cast(null as VARCHAR(850))                              as NAME_CHAR,
    cast(null as text)                                      as PROVIDER_BLOB,
    CURRENT_TIMESTAMP                                       as UPDATE_DATE,
    CURRENT_TIMESTAMP                                       as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                       as IMPORT_DATE,
    cast(null 	as VARCHAR(50))                             as SOURCESYSTEM_CD,
    cast(null	as INT)                                     as UPLOAD_ID
from {source_schema}.V_DEID_PROVIDER as dim;