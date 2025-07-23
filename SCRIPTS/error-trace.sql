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