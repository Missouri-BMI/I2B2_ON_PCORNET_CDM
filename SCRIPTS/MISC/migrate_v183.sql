use role i2b2;
use warehouse i2b2_etl_wh;

use database i2b2_etl_test;

drop schema i2b2data;
drop schema i2b2hive;
drop schema i2b2pm;
drop schema i2b2metadata;
drop schema i2b2workdata;

-- install data from airflow

---dev

--i2b2
use database i2b2_dev;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;



--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_dev.i2b2data_v183 TO ROLE i2b2_dev_app_role;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_dev.i2b2metadata_v183 TO ROLE i2b2_dev_app_role;


--shrine-Mu
use database i2b2_shrine_mu_dev;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;


--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_mu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_mu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_MU_APP_ROLE;


--shrine-WASHU
use database i2b2_shrine_washu_dev;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;



--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_washu_dev.i2b2data_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_washu_dev.i2b2metadata_v183 TO ROLE SHRINE_DEV_WASHU_APP_ROLE;


-- run data refresh for each site

--prod


--i2b2
use database i2b2_prod;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;



--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_prod.i2b2data_v183 TO ROLE i2b2_prod_app_role;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_prod.i2b2metadata_v183 TO ROLE i2b2_prod_app_role;


--shrine-Mu
use database i2b2_shrine_mu_prod;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;


--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_mu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_mu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_MU_APP_ROLE;


--shrine-WASHU
use database i2b2_shrine_washu_prod;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;



--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_washu_prod.i2b2data_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_shrine_washu_prod.i2b2metadata_v183 TO ROLE SHRINE_PROD_WASHU_APP_ROLE;

--gpc

use database i2b2_gpc;

create or replace schema i2b2data_V183 clone i2b2_etl_test.i2b2data;
create or replace schema i2b2metadata_V183 clone i2b2_etl_test.i2b2metadata;



--data
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_gpc.i2b2data_v183 TO ROLE I2B2_GPC;


--metadata
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;
GRANT  ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;


GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;


GRANT USAGE ON ALL SEQUENCES IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA i2b2_gpc.i2b2metadata_v183 TO ROLE I2B2_GPC;

-- run data refresh for each site


--mu
insert into i2b2_prod.i2b2data_v183.QT_XML_RESULT 
select * from  I2B2_PROD.I2B2DATA.QT_XML_RESULT;

insert into i2b2_prod.i2b2data_v183.QT_QUERY_RESULT_INSTANCE 
select * from I2B2_PROD.I2B2DATA.QT_QUERY_RESULT_INSTANCE;

insert into i2b2_prod.i2b2data_v183.QT_QUERY_MASTER 
select * from  I2B2_PROD.I2B2DATA.QT_QUERY_MASTER;


insert into i2b2_prod.i2b2data_v183.QT_QUERY_INSTANCE 
select * from   I2B2_PROD.I2B2DATA.QT_QUERY_INSTANCE;

insert into i2b2_prod.i2b2data_v183.QT_PDO_QUERY_MASTER 
select * from   I2B2_PROD.I2B2DATA.QT_PDO_QUERY_MASTER;


--gpc
insert into i2b2_gpc.i2b2data_v183.QT_XML_RESULT 
select * from  i2b2_sandbox_gpc.I2B2DATA.QT_XML_RESULT;

insert into i2b2_gpc.i2b2data_v183.QT_QUERY_RESULT_INSTANCE 
select * from  i2b2_sandbox_gpc.I2B2DATA.QT_QUERY_RESULT_INSTANCE;

insert into i2b2_gpc.i2b2data_v183.QT_QUERY_MASTER 
select * from  i2b2_sandbox_gpc.I2B2DATA.QT_QUERY_MASTER;


insert into i2b2_gpc.i2b2data_v183.QT_QUERY_INSTANCE 
select * from  i2b2_sandbox_gpc.I2B2DATA.QT_QUERY_INSTANCE;

insert into i2b2_gpc.i2b2data_v183.QT_PDO_QUERY_MASTER 
select * from  i2b2_sandbox_gpc.I2B2DATA.QT_PDO_QUERY_MASTER;



create or replace table i2b2_prod.i2b2metadata_v183.ACT_MED_LIST_VA_V42 as
select * from i2b2_prod.i2b2metadata_v183.ACT_MED_VA_V42;

-- 
update i2b2_prod.i2b2metadata_v183.ACT_MED_LIST_VA_V42
set c_fullname = replace(c_fullname, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_dimcode = replace(c_dimcode, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_totalnum = null 
, c_facttablecolumn = 'med_list_fact.concept_cd';

-- concept_path
delete from i2b2_prod.i2b2data_v183.concept_dimension
where concept_path like '\\ACT\\HomeMedications\\%';

insert into i2b2_prod.i2b2data_v183.concept_dimension
with all_med as (
    select * from i2b2_prod.i2b2data_v183.concept_dimension
    where concept_path like '\\ACT\\Medications\\MedicationsByVaClass\\%'
)
select 
    replace(concept_path, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\') concept_path
    , concept_cd
    , name_char
    , concept_blob
    , update_date
    , download_date
    , import_date
    , sourcesystem_cd
    , upload_id
from all_med;


-- table access
delete from i2b2_prod.i2b2metadata_v183.table_access
where C_TABLE_CD = 'ACT_MED_LIST_VA_2018';

insert into i2b2_prod.i2b2metadata_v183.table_access
select 
    'ACT_MED_LIST_VA_2018' as C_TABLE_CD,
    'ACT_MED_LIST_VA_V42' as C_TABLE_NAME,
    C_PROTECTED_ACCESS,
    C_ONTOLOGY_PROTECTION,
    C_HLEVEL,
    C_FULLNAME,
    'ACT Home Medications VA Classes' C_NAME,
    C_SYNONYM_CD,
    c_visualattributes,
    null as c_totalnum,
    c_basecode,
    c_metadataxml,
    'med_list_fact.concept_cd' as c_facttablecolumn,
    c_dimtablename,
    c_columnname,
    c_columndatatype,
    c_operator,
    c_dimcode,
    c_comment,
    'ACT Home Medications' as c_tooltip,
    c_entry_date,
    c_change_date,
    c_status_cd,
    valuetype_cd
from i2b2_prod.i2b2metadata_v183.table_access 
where c_table_cd = 'ACT_MED_VA_2018';

update i2b2_prod.i2b2metadata_v183.table_access
set c_fullname = replace(c_fullname, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_dimcode = replace(c_dimcode, '\\ACT\\Medications\\', '\\ACT\\HomeMedications\\')
, c_totalnum = null 
where C_TABLE_CD = 'ACT_MED_LIST_VA_2018';


create or replace table  i2b2_prod.i2b2data_v183.med_list_fact as
select
    cast(ENCOUNTERID as NUMBER(38, 0))                                                              as ENCOUNTER_NUM, 
    cast(PATID as NUMBER(38, 0))                                                                    as PATIENT_NUM, 
    concat('RXNORM:', RXNORM_CUI)                                                                   as CONCEPT_CD,
    COALESCE(fact.RX_PROVIDERID, '@')                                                               as PROVIDER_ID, 
    TO_TIMESTAMP(RX_ORDER_DATE   :: DATE || ' ' || RX_ORDER_TIME, 'YYYY-MM-DD HH24:MI:SS')          as START_DATE,  
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
    cast(null as VARCHAR(50))                                                               as SOURCESYSTEM_CD,                                                                    
    cast(null as  integer)                                                                          as UPLOAD_ID
from DEIDENTIFIED_PCORNET_CDM.cdm.DEID_MED_LIST fact
where RXNORM_CUI is not null;


insert into i2b2_prod.i2b2metadata_v183.table_access
select * from i2b2_prod.i2b2metadata.table_access
where c_fullname like  '%DocumentOntology%';


create or replace table i2b2_prod.i2b2metadata_v183.document_ontology as
select * from i2b2_prod.i2b2metadata.document_ontology;

insert into i2b2_prod.i2b2data_v183.concept_dimension
select * from i2b2_prod.i2b2data.concept_dimension
where concept_path like  '%DocumentOntology%';