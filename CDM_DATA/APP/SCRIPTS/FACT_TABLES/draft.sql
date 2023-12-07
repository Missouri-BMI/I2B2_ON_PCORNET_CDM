-- 6.8k
select c_hlevel, c_name, c_basecode, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_basecode like 'NAACCR|%'
order by c_hlevel;

-- 102





select * from deidentified_pcornet_cdm.cdm_2023_oct.deid_tumor


select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT' 
    and lower(column_name) like lower('%_N%') 
    and lower(column_name) not like lower('RAW_%')
    and lower(column_name) not like '%date%'
order by column_name;

select  patid, count(*) cn from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR
group by patid
order by cn desc;

select  count(*) cn from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR
where patid is null;

select * from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR
where patid = 679224;



// all base codes
select distinct c_basecode from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY;

//if NAACCR, CS
select distinct split_part(c_basecode, '|', 1) from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY;



select c_hlevel, c_name, c_basecode, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_fullname like '\\i2b2\\naaccr\\csterms\\%'
order by c_hlevel;

select c_hlevel, c_name, c_basecode, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_basecode like 'NAACCR|%'
order by c_hlevel;


select c_hlevel, c_name, split_part(c_basecode, '|', 2) code, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_basecode like 'NAACCR|%' and code like '3110%'
order by c_hlevel;

--56296
select distinct ADDR_AT_DX_STATE_N80 from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR;

-- NAACCR

select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT' 
    and lower(column_name) like lower('%_N%') and lower(column_name) not like lower('RAW_%')
order by column_name;




-- seer_site

select c_hlevel, c_name, c_basecode, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_basecode like 'SEER_SITE:%'
order by c_hlevel, c_basecode;
--21047

select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT' 
    and lower(column_name) like lower('%seer_site%')
order by column_name;
-- SEER_SITE_SPECIFIC_FACT_N3700
-- SEER_SITE_SPECIFIC_FACT_N3702
-- SEER_SITE_SPECIFIC_FACT_N3704
-- SEER_SITE_SPECIFIC_FACT_N3706
-- SEER_SITE_SPECIFIC_FACT_N3708
-- SEER_SITE_SPECIFIC_FACT_N3710


select column_name, split_part(column_name, '_N', 1) first_part, split_part(column_name, '_N', -1) last_part from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT';

-- all
select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT'
    and lower(column_name) like lower('%_N%') and lower(column_name) not like lower('RAW_%');

-- SEER_SITE
select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT'
    and lower(column_name) like lower('%_N%')
    and lower(column_name) like '%seer_site%';

select distinct SEER_SITE_SPECIFIC_FACT_N3706 
, SEER_SITE_SPECIFIC_FACT_N3700
, SEER_SITE_SPECIFIC_FACT_N3702
, SEER_SITE_SPECIFIC_FACT_N3704
, SEER_SITE_SPECIFIC_FACT_N3706
, SEER_SITE_SPECIFIC_FACT_N3708
, SEER_SITE_SPECIFIC_FACT_N3710
from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR; 

-- CS
select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT' 
    and lower(column_name) like lower('%cs_%')
order by column_name;

--11.7k
select c_hlevel, c_name, c_basecode, c_fullname from I2B2_DEV.I2B2METADATA.NAACCR_ONTOLOGY
where c_basecode like 'CS|%'
order by c_hlevel;



select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT' 
    and lower(column_name) like lower('%cs_%')
order by column_name;



select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT'
    and lower(column_name) like lower('%pat%');


select column_name from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT'
    and lower(column_name) like lower('%date%')
order by column_name;
    


select distinct  from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR
where patid = 679224;


SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_name) from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
where 
    table_name = 'DEID_TUMOR' 
    and table_schema = 'CDM_2023_OCT'
    and lower(column_name) like lower('%_N%') and lower(column_name) not like lower('RAW_%');




select patid, DATE_CASE_INITIATED_N2085, ADDR_AT_DX_STATE_N80, ADDR_AT_DX_CITY_N70 from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR 
where patid = 1000557;
-- 1000557	2020-01-29	MO	Webb City
-- 1000557	2018-09-01	MO	Webb City
-- 1000557	2020-10-13	MO	Webb City
-- 1000557	2018-04-01	MO	Webb City


SELECT
    patid
    , concat('NAACCR|', split_part(concept, '_N', -1), ':', coalesce(null, '')) as concept_cd  
    from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR 
UNPIVOT(concept_cd FOR concept IN (RECORD_TYPE_N10))
ORDER BY patid;


select distinct RECORD_TYPE_N10 from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR

 SELECT distinct data_type from DEIDENTIFIED_PCORNET_CDM.information_schema.columns
    where 
        table_name = 'DEID_TUMOR' 
        and table_schema = 'CDM_2023_OCT'
        and lower(column_name) like lower('%_N%') 
        and lower(column_name) not like lower('RAW_%');


        select * from i2b2_dev.i2b2metadata.table_access;

         select * from i2b2_dev.i2b2metadata.NAACCR_ONTOLOGY;

        update i2b2_dev.i2b2metadata.table_access
        set c_facttablecolumn = 'tumor_fact.concept_cd'
        where c_table_name = 'NAACCR_ONTOLOGY';

        update i2b2_dev.i2b2metadata.NAACCR_ONTOLOGY
        set c_facttablecolumn = 'tumor_fact.concept_cd';



         insert into i2b2data.concept_dimension (CONCEPT_PATH, CONCEPT_CD, NAME_CHAR, CONCEPT_BLOB, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD, UPLOAD_ID)
    select 
        C_FULLNAME
        , C_BASECODE
        , C_BASECODE
        , NULL
        , CURRENT_DATE
        , CURRENT_DATE
        , CURRENT_DATE
        , NULL
        , NULL
    FROM i2b2_dev.i2b2metadata.NAACCR_ONTOLOGY
    WHERE C_BASECODE IS NOT NULL;


    call i2b2_dev.i2b2metadata.RUNTOTALNUM('TUMOR_FACT', 'I2B2DATA', 'NAACCR_ONTOLOGY');


    select count(distinct patid) from DEIDENTIFIED_PCORNET_CDM.CDM_2023_OCT.DEID_TUMOR; 