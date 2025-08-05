USE SCHEMA {{ wd_schema }};

{% if project == 'mu' %}
-- MU
select 1;

{% elif project == 'washu' %}
-- washu
select 1;

{% elif project == 'gpc' %}
-- gpc
select 1;
{% elif project == 'pcornet' %}
-- pcornet
select 1;
{% endif %}
