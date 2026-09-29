use schema {{ metadata_schema }};
{% if site == 'mu' %}
-- MU
select 1;

{% elif site == 'washu' %}
-- washu
select 1;

{% elif site == 'gpc' %}
-- gpc
select 1;
{% elif site == 'pcornet' %}
-- pcornet
select 1;
{% endif %}
