#!/bin/bash

# Read DB connection info from environment variables
# Example usage:
#   export PGHOST="i2b2-db.ctsvcfrduobf.us-east-2.rds.amazonaws.com"
#   export PGUSER="postgres"
#   export PGPASSWORD="yourpassword"
#   export PGDATABASE="postgres"

: "${PGHOST:?PGHOST env variable not set}"
: "${PGUSER:?PGUSER env variable not set}"
: "${PGPASSWORD:?PGPASSWORD env variable not set}"
: "${PGDATABASE:=postgres}"

export PGPASSWORD

psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "DROP DATABASE IF EXISTS i2b2;"
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "CREATE DATABASE i2b2;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2data;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2hive;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2pm;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2metadata;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2workdata;"
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -c "CREATE SCHEMA IF NOT EXISTS i2b2imdata;"

./i2b2-data/edu.harvard.i2b2.data/Release_1-8/apache-ant/bin/ant \
    -f ./i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/build.xml \
    create_database load_demodata

echo "Database i2b2 created and data loaded successfully."
echo "Running post-installation scripts 1..."
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -v ON_ERROR_STOP=1 <<EOF
    SET search_path TO i2b2hive;

    UPDATE hive_cell_params
    SET value = '11'
    WHERE param_name_cd = 'edu.harvard.i2b2.crc.setfinderquery.obfuscation.minimum.value';

    UPDATE hive_cell_params
    SET value = 1000
    WHERE param_name_cd = 'edu.harvard.i2b2.crc.analysis.queue.large.maxjobcount';

    UPDATE hive_cell_params
    SET value = 1000
    WHERE param_name_cd = 'edu.harvard.i2b2.crc.analysis.queue.medium.maxjobcount';

    UPDATE hive_cell_params
    SET value = 1000
    WHERE param_name_cd = 'edu.harvard.i2b2.crc.lockout.setfinderquery.count';

    UPDATE hive_cell_params
    SET value = 'true'
    WHERE param_name_cd = 'queryprocessor.multifacttable';
EOF

echo "Running post-installation scripts 2..."
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -v ON_ERROR_STOP=1 <<EOF
    SET search_path TO i2b2pm;

    truncate pm_cell_data;

    INSERT INTO PM_CELL_DATA (CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
    VALUES('CRC', '/', 'Data Repository', 'REST', 'http://localhost/i2b2/services/QueryToolService/', 1, 'A');
    
    INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
    VALUES('FRC', '/', 'File Repository ', 'SOAP', 'http://localhost/i2b2/services/FRService/', 1, 'A');
    
    INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
    VALUES('ONT', '/', 'Ontology Cell', 'REST', 'http://localhost/i2b2/services/OntologyService/', 1, 'A');
    
    INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
    VALUES('WORK', '/', 'Workplace Cell', 'REST', 'http://localhost/i2b2/services/WorkplaceService/', 1, 'A');
    
    INSERT INTO PM_CELL_DATA(CELL_ID, PROJECT_PATH, NAME, METHOD_CD, URL, CAN_OVERRIDE, STATUS_CD)
    VALUES('IM', '/', 'IM Cell', 'REST', 'http://localhost/i2b2/services/IMService/', 1, 'A');

    TRUNCATE pm_user_data;

    INSERT INTO pm_user_data (user_id, full_name, password, status_cd)
    VALUES('AGG_SERVICE_ACCOUNT', 'AGG_SERVICE_ACCOUNT', '9117d59a69dc49807671a51f10ab7f', 'A');

    INSERT INTO pm_user_data (user_id, full_name, password, status_cd)
    VALUES('mhmcb@umsystem.edu', 'Md Saber Hossain', '9117d59a69dc49807671a51f10ab7f', 'A');

    TRUNCATE pm_project_user_roles;

    INSERT INTO pm_project_user_roles (project_id, user_id, user_role_cd, status_cd)
    VALUES('@', 'mhmcb@umsystem.edu', 'ADMIN', 'A');

    TRUNCATE pm_user_params;

    INSERT INTO pm_user_params (datatype_cd, user_id, param_name_cd, value, change_date, entry_date, status_cd)
    VALUES('T', 'mhmcb@umsystem.edu', 'authentication_method', 'SAML', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'A');
EOF

echo "Running post-installation scripts 3..."
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -v ON_ERROR_STOP=1 <<EOF
    SET search_path TO i2b2workdata;

    INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
      VALUES('act', 'WORKPLACE','N', 0, 'SHARED', 'shared', 'ACT', 'Y', 100, NULL, 'CA', 'SHARED');

    INSERT INTO WORKPLACE_ACCESS(C_TABLE_CD, C_TABLE_NAME, C_PROTECTED_ACCESS, C_HLEVEL, C_NAME, C_USER_ID, C_GROUP_ID, C_SHARE_ID, C_INDEX, C_PARENT_INDEX, C_VISUALATTRIBUTES, C_TOOLTIP)
     VALUES('act', 'WORKPLACE','N', 0, '@', '@', '@', 'N', 0, NULL, 'CA', '@');
EOF

## project specific changes
echo "Running post-installation scripts 4..."
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -v ON_ERROR_STOP=1 <<EOF
    SET search_path TO i2b2hive;

    UPDATE crc_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE im_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE ont_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
    UPDATE work_db_lookup SET C_DOMAIN_ID = 'nextgenbmi.umsystem.edu' WHERE C_DOMAIN_ID = 'i2b2demo';
EOF

echo "Running post-installation scripts 5..."
psql -h "$PGHOST" -U "$PGUSER" -d i2b2 -v ON_ERROR_STOP=1 <<EOF
    SET search_path TO i2b2pm;
    UPDATE pm_hive_data SET DOMAIN_NAME = 'nextgenbmi.umsystem.edu' WHERE DOMAIN_NAME = 'i2b2demo';
    --
    DELETE FROM pm_project_user_roles where project_id = 'ACT';

    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'USER', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'MANAGER', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_OBFSC', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'AGG_SERVICE_ACCOUNT', 'DATA_AGG', 'A');
    
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'USER', 'A');

    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'MANAGER', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'EDITOR', 'A');

    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_DEID', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_OBFSC', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_AGG', 'A');
    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_LDS', 'A');

    INSERT INTO PM_PROJECT_USER_ROLES (PROJECT_ID, USER_ID, USER_ROLE_CD, STATUS_CD)
    VALUES('ACT', 'mhmcb@umsystem.edu', 'DATA_PROT', 'A');
EOF

echo "Running post-installation Done..."