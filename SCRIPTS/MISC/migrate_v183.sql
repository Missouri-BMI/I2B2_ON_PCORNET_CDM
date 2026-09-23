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
use database i2b2_dev;
create schema i2b2data_V183;
create schema i2b2metadata_V183;

--shrine-Mu
use database i2b2_shrine_mu_dev;
create schema i2b2data_V183;
create schema i2b2metadata_V183;

--shrine-WASHU
use database i2b2_shrine_washu_dev;
create schema i2b2data_V183;
create schema i2b2metadata_V183;



select * from i2b2_shrine_washu_dev.i2b2metadata_v183.table_access;



select value from i2b2_dev.I2B2DATA_v183.QT_BREAKDOWN_PATH


select value from i2b2_prod.I2B2DATA.QT_BREAKDOWN_PATH