use database CDM_DATALAKE;
use role i2b2;

CALL CDM_DATALAKE.PUBLIC.RUN_GPC_ETL_PROCESS(
        'DEIDENTIFIED_GROUSE_DB',
        'PCORNET_CDM_',
        'V_',
        'GPC_',
        'PCORNET_CDM_MU',
        'CDM_DATALAKE',
        'GPC',
        NULL   -- SITE_DATAMARTID_FILTER: NULL = all sites
    );

CALL CDM_DATALAKE.PUBLIC.RUN_GPC_ETL_PROCESS(
        'DEIDENTIFIED_GROUSE_DB',
        'PCORNET_CDM_',
        'V_',
        'GPC_',
        'PCORNET_CDM_MU',
        'CDM_DATALAKE',
        'WASHU',
        'C4WU' -- SITE_DATAMARTID_FILTER
    );    

