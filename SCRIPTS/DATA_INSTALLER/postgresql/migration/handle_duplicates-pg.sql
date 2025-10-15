--318
--334
select count(*) from i2b2pm.pm_user_data;
create  table i2b2pm.pm_user_data_backup as 
select * from i2b2pm.pm_user_data;

select * from i2b2pm.pm_user_data_backup where user_id not in (select user_id from i2b2pm.pm_user_data);

insert into i2b2pm.pm_user_data
select * from i2b2pm.pm_user_data_backup where user_id not in (select user_id from i2b2pm.pm_user_data);



--1692
--1784
select count(*) from i2b2pm.pm_project_user_roles;
create  table i2b2pm.pm_project_user_roles_backup as 
select * from i2b2pm.pm_project_user_roles;

select * from i2b2pm.pm_project_user_roles_backup where user_id not in (select user_id from i2b2pm.pm_project_user_roles)

insert into i2b2pm.pm_project_user_roles
select * from i2b2pm.pm_project_user_roles_backup where user_id not in (select user_id from i2b2pm.pm_project_user_roles);


--321
select count(*) from i2b2pm.pm_user_params;

truncate i2b2pm.pm_user_params;

insert into i2b2pm.pm_user_params(DATATYPE_CD, USER_ID, PARAM_NAME_CD, VALUE, STATUS_CD)
select 
'T'
, user_id
, 'authentication_method'
, 'SAML'
, 'A'
from i2b2pm.pm_user_data
where user_id <> 'AGG_SERVICE_ACCOUNT';