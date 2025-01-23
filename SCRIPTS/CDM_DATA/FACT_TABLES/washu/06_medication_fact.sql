create or replace view  {target_schema}.MEDICATION_FACT as
select
    ec.ENCOUNTER_NUM                                                                                as ENCOUNTER_NUM, 
    pc.PATIENT_NUM                                                                                  as PATIENT_NUM, 
    concat('RXNORM:', RXNORM_CUI)                                                                   as CONCEPT_CD,
    COALESCE(fact.RX_PROVIDERID, '@')                                                               as PROVIDER_ID, 
    RX_ORDER_DATE :: TIMESTAMP                                                                      as START_DATE,
    '@'                                                                                             as MODIFIER_CD,
    1                                                                                               as INSTANCE_NUM, 
    cast('' as VARCHAR(50))                                                                         as VALTYPE_CD,
    cast('' as VARCHAR(255))                                                                        as TVAL_CHAR, 
    cast(null as DECIMAL(18, 5))                                                                    as NVAL_NUM, 
    ''                                                                                              as VALUEFLAG_CD,
    cast(null as  integer)                                                                          as QUANTITY_NUM, 
    cast('@' as VARCHAR(50))                                                                        as UNITS_CD, 
    RX_END_DATE :: TIMESTAMP                                                                        as END_DATE, 
    '@'                                                                                             as LOCATION_CD, 
    cast(null as  text)                                                                             as OBSERVATION_BLOB, 
    cast(null as  integer)                                                                          as CONFIDENCE_NUM, 
    CURRENT_TIMESTAMP                                                                               as UPDATE_DATE,
    CURRENT_TIMESTAMP                                                                               as DOWNLOAD_DATE,
    CURRENT_TIMESTAMP                                                                               as IMPORT_DATE,
    cast(null as VARCHAR(50))                                                                       as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from {source_schema}.V_DEID_PRESCRIBING fact
left join CDM_DATALAKE.GPC.ENCOUNTER_CROSSWALK as ec
on fact.encounterid = ec.encounterid and ec.pcornet_site_name = 'C4WU'
left join CDM_DATALAKE.GPC.PATIENT_CROSSWALK as pc
on fact.patid = pc.patid and  pc.pcornet_site_name = 'C4WU'
where RXNORM_CUI is not null
;
