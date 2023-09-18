--- Lockout after failed logins
SET SEARCH_PATH TO i2b2pm;
TRUNCATE pm_global_params;
TRUNCATE pm_user_params;

INSERT 
    INTO pm_global_params 
        (DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
    VALUES
        ('T', 'PM_LOCKED_MAX_COUNT', '/', '5', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'A');
INSERT 
    INTO pm_global_params 
        (DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
    VALUES
        ('T', 'PM_LOCKED_WAIT_TIME', '/', '262080', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'A');


-- PASSWORD EXPIRED
INSERT 
    INTO pm_global_params 
        (DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
    VALUES
        ('T', 'PM_EXPIRED_PASSWORD', '/', '180', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'A');


--- PASSWORD RULE
INSERT 
    INTO pm_global_params 
        (DATATYPE_CD, PARAM_NAME_CD, PROJECT_PATH, VALUE, CAN_OVERRIDE, CHANGE_DATE, ENTRY_DATE, CHANGEBY_CHAR, STATUS_CD)
    VALUES
        ('T', 'PM_COMPLEX_PASSWORD', '/', '(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[)(;:}{,.!@#$%^+=])(?=\S+$).{8,}', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'A');



-- Unlock Account
DELETE
	FROM pm_user_login
WHERE
	USER_ID = 'mhmcb' AND
	(ATTEMPT_CD = 'LOCKED_OUT' OR
	ATTEMPT_CD = 'BADPASSWORD');



-- restore deleted account
update i2b2pm.pm_user_data
set change_date = CURRENT_TIMESTAMP, changeby_char = 'mhmcb@umsystem.edu', status_cd = 'A'
where user_id = 'lrwq3w@umsystem.edu'

-- SAML ENABLE FOR existing users

TRUNCATE pm_global_params; -- optionally delete global params assoicated with LOCAL authentication system.
TRUNCATE pm_user_params; -- optionally delete pm_user_parms associated with LOCAL authentication.

Insert Into i2b2pm.pm_user_params (datatype_cd, user_id, param_name_cd, value, change_date, entry_date, changeby_char, status_cd)
SELECT 
	'T', 
	user_id,
	'authentication_method',
	'SAML',
	CURRENT_TIMESTAMP,
	CURRENT_TIMESTAMP,
	NULL,
	'A'
FROM i2b2pm.pm_user_data
WHERE user_id != 'AGG_SERVICE_ACCOUNT';
	
---


    delete from pm_project_user_roles where project_id = 'ACT' and user_id !='AGG_SERVICE_ACCOUNT';

delete from pm_project_user_roles  where user_id ='i2b2';

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd) 
    select 'ACT', user_id, 'USER', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from pm_user_data where user_id != 'AGG_SERVICE_ACCOUNT';

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd) 
select 'ACT', user_id, 'DATA_AGG', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from pm_user_data where user_id != 'AGG_SERVICE_ACCOUNT';

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd) 
select 'ACT', user_id, 'DATA_OBFSC', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from pm_user_data where user_id != 'AGG_SERVICE_ACCOUNT';

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd) 
select 'ACT', user_id, 'DATA_LDS', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from pm_user_data where user_id != 'AGG_SERVICE_ACCOUNT';

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd) 
select 'ACT', user_id, 'DATA_DEID', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from pm_user_data where user_id != 'AGG_SERVICE_ACCOUNT';



insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd)
with admins as (
    select distinct user_id from pm_project_user_roles where project_id = '@' and user_role_cd = 'ADMIN'
)
select 'ACT', user_id, 'DATA_PROT', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from admins 

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd)
with admins as (
    select distinct user_id from pm_project_user_roles where project_id = '@' and user_role_cd = 'ADMIN'
)
select 'ACT', user_id, 'EDITOR', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from admins ;

insert into pm_project_user_roles(project_id, user_id, user_role_cd, change_date, entry_date, changeby_char, status_cd)
with admins as (
    select distinct user_id from pm_project_user_roles where project_id = '@' and user_role_cd = 'ADMIN'
)
select 'ACT', user_id, 'MANAGER', current_timestamp, current_timestamp,'mhmcb@umsystem.edu', 'A' from admins ;


