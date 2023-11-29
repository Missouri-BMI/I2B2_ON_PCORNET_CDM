-- create view
create or replace view #target_schema.PROVIDER_DIMENSION
as 
select 
    PROVIDERID                                              as PROVIDER_ID,
    PROVIDER_SEX,
    cast('@'  as VARCHAR(700))                              as PROVIDER_PATH,
    cast(null as VARCHAR(850))                              as NAME_CHAR,
    cast(null as text)                                      as PROVIDER_BLOB,
    PROVIDER_SPECIALTY_PRIMARY,
    PROVIDER_NPI,
    PROVIDER_NPI_FLAG,
    RAW_PROVIDER_SPECIALTY_PRIMARY,
    CURRENT_TIMESTAMP                                       as UPDATE_DATE,
    CURRENT_TIMESTAMP                                       as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                       as IMPORT_DATE,
    cast(null 	as VARCHAR(50))                             as SOURCESYSTEM_CD,
    cast(null	as INT)                                     as UPLOAD_ID
from #source_schema.GPC_PROVIDER as dim;