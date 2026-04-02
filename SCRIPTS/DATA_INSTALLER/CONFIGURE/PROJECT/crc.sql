use schema {{ crc_schema }};

{% if site == 'mu' %}
-- MU
select 1;

{% elif site == 'washu' %}
-- washu
select 1;

{% elif site == 'gpc' %}
-- gpc

INSERT INTO QT_BREAKDOWN_PATH(NAME,VALUE,CREATE_DATE)
VALUES ('PATIENT_GPCSITE_COUNT_XML','\\\\ACT_DEMO\\ACT\\Demographics\\GPC Sites\\', CURRENT_TIMESTAMP);

INSERT INTO QT_QUERY_RESULT_TYPE(RESULT_TYPE_ID,NAME,DESCRIPTION,DISPLAY_TYPE_ID,VISUAL_ATTRIBUTE_TYPE_ID,CLASSNAME) 
VALUES (21, 'PATIENT_GPCSITE_COUNT_XML','GPC Site breakdown','CATNUM','LA','edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator');

{% elif site == 'pcornet' %}
-- pcornet

INSERT INTO QT_QUERY_RESULT_TYPE(RESULT_TYPE_ID,NAME,DESCRIPTION,DISPLAY_TYPE_ID,VISUAL_ATTRIBUTE_TYPE_ID,CLASSNAME) 
VALUES (21,'PATIENT_PCORNETSITE_COUNT_XML','PCORNet Site breakdown','CATNUM','LA','edu.harvard.i2b2.crc.dao.setfinder.QueryResultGenerator');

INSERT INTO QT_BREAKDOWN_PATH(NAME,VALUE,CREATE_DATE) 
VALUES ('PATIENT_PCORNETSITE_COUNT_XML','\\\\ACT_DEMO\\ACT\\Demographics\\PCORNet Sites\\', CURRENT_TIMESTAMP);
{% endif %}
