call I2B2_PROD.I2B2METADATA.RUN_ON_ALL_FACT();



update i2b2pm.pm_project_data
set project_name =  'NextGen Data Lake De-Identified' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from DEIDENTIFIED_PCORNET_CDM.CDM.HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'ACT';


call I2B2_SANDBOX_GPC.I2B2METADATA.RUN_ON_ALL_FACT();


update i2b2pm.pm_project_data
set project_name =  'GPC Data Lake' || ' (' || (
    select TO_CHAR(MAX(REFRESH_ENCOUNTER_DATE), 'MMMM YYYY') from CDM_DATALAKE.GPC.GPC_DEID_HARVEST
    where datamartid = 'C4UMO'
) || ')'
where project_id = 'SANDBOX-GPC';


-- call I2B2_SANDBOX_GPC.I2B2METADATA.RUNTOTALNUM('tumor_fact', 'i2b2data', 'NAACCR_ONTOLOGY');