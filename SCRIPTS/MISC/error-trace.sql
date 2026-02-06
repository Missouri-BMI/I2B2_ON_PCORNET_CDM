use database i2b2_shrine_washu_prod;
use schema i2b2data;

select qqm.query_master_id, qqm.create_date, qqm.name, qqri.message, qqm.generated_sql from qt_query_master as qqm
left join QT_QUERY_INSTANCE as qqi
using (QUERY_MASTER_ID)
left join QT_QUERY_RESULT_INSTANCE as qqri
using(query_instance_id)
left join QT_QUERY_STATUS_TYPE as qqst
on qqst.status_type_id = qqri.status_type_id
where qqst.status_type_id != 3
order by qqm.create_date desc;


CREATE TEMP  TABLE QUERY_GLOBAL_TEMP (  ENCOUNTER_NUM int,  PATIENT_NUM int, INSTANCE_NUM int, CONCEPT_CD varchar(50), START_DATE TIMESTAMP, PROVIDER_ID varchar(50),  PANEL_COUNT int,  fact_count int,  fact_panels int );

 CREATE TEMP TABLE DX  (  ENCOUNTER_NUM int,  PATIENT_NUM int, INSTANCE_NUM int, CONCEPT_CD varchar(50), START_DATE TIMESTAMP, PROVIDER_ID varchar(50), temporal_start_date TIMESTAMP, temporal_end_date TIMESTAMP ) ;

  CREATE TEMP TABLE MASTER_QUERY_GLOBAL_TEMP  (  ENCOUNTER_NUM int,  PATIENT_NUM int , INSTANCE_NUM int, CONCEPT_CD varchar(50), START_DATE TIMESTAMP, PROVIDER_ID varchar(50), MASTER_ID varchar(50), LEVEL_NO int, temporal_start_date TIMESTAMP, temporal_end_date TIMESTAMP ) ;

  insert into QUERY_GLOBAL_TEMP (patient_num, panel_count)
with t as ( 
 select  f.patient_num  
from I2B2DATA.tumor_fact f 
where  
f.concept_cd IN (select concept_cd from  I2B2DATA.CONCEPT_DIMENSION   where CONCEPT_PATH LIKE '\\i2b2\\naaccr\\Stage/Prognostic Factors\\seerSummaryStage2000\\%')   
group by  f.patient_num 
 ) 
select  t.patient_num, 0 as panel_count  from t 
;


insert into QUERY_GLOBAL_TEMP (patient_num, panel_count)
with t as ( 
 select  f.patient_num  
from I2B2DATA.tumor_fact f 
where  
f.concept_cd IN (select concept_cd from  I2B2DATA.CONCEPT_DIMENSION   where CONCEPT_PATH LIKE '\\i2b2\\naaccr\\Stage/Prognostic Factors\\derivedSummaryStage2018\\%')   
group by  f.patient_num 
 ) 
select  t.patient_num, 0 as panel_count  from t 
;


insert into QUERY_GLOBAL_TEMP (patient_num, panel_count)
with t as ( 
 select  f.patient_num  
from I2B2DATA.tumor_fact f 
where  
f.concept_cd IN (select concept_cd from  I2B2DATA.CONCEPT_DIMENSION   where CONCEPT_PATH LIKE '\\i2b2\\naaccr\\Stage/Prognostic Factors\\seerSummaryStage2000\\%')   
group by  f.patient_num 
 ) 
select  t.patient_num, 0 as panel_count  from t 
;