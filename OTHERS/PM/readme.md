## Create New User

### Step 1
Enter user data

```
INSERT INTO PM_USER_DATA (USER_ID, FULL_NAME, PASSWORD, STATUS_CD)
VALUES('mhmcb@umsystem.edu', 'Md Saber Hossain', '9117d59a69dc49807671a51f10ab7f', 'A');
```

### Step 2
Add user saml auth info
```
INSERT INTO pm_user_params (datatype_cd, user_id, param_name_cd, value, change_date, entry_date, changeby_char, status_cd)
VALUES( 'T', 'mhmcb@umsystem.edu', 'authentication_method', 'SAML', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 'A')
```

### Step 3
Add project specific user roles
```
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'USER', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_DEID', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_OBFSC', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_AGG', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_LDS', 'A');
```

### Step 4 (Optional)
Add user as i2b2 admin
```
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('@', 'mhmcb@umsystem.edu', 'ADMIN', 'A'); 
```

### Step 5 (Optional)
Add user as project admin
```
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'EDITOR', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_PROT', 'A');
INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
VALUES('ACT', 'mhmcb@umsystem.edu', 'MANAGER', 'A');
```


