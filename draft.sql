select  qqri.result_instance_id, qqrt.NAME ,qxr.xml_value from qt_query_master as qqm
left join qt_query_instance as qqi
on qqm.query_master_id = qqi.query_master_id
left join QT_QUERY_RESULT_INSTANCE as qqri
on qqi.QUERY_INSTANCE_ID = qqri.QUERY_INSTANCE_ID
left join qt_query_result_type as qqrt
on qqri.result_type_id = qqrt.result_type_id
left join qt_xml_result as qxr
on qxr.result_instance_id = qqri.result_instance_id
where qqm.query_master_id = 1
order by qqri.result_instance_id;