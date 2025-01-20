from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import os
from airflow.utils.task_group import TaskGroup
from dotenv import dotenv_values
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from common import *

with DAG(
    "i2b2_data_install",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["mhmcb@missouri.edu"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="i2b2 data loader in Snowflake",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["i2b2_data_install"],
) as dag:
    
    args = dotenv_values("/opt/airflow/env/dev/.env")
    
    project = args['PROJECT']
    snowflake_conn_id = 'mu-dev-init'

    BASE_PATH = '/opt/airflow/SCRIPTS/DATA_INSTALLER'
    DATA_INSTALLER_PATH = f"{BASE_PATH}/i2b2-data"
    ANT_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/apache-ant/bin/ant"
    ANT_BUILD_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/NewInstall/build.xml"
    
    soruce_db=args['SOURCE_DB']
    source_schema = soruce_db + '.' + args['SOURCE_SCHEMA']

    target_db= args['TARGET_DB']
    target_schema = target_db + '.' + args['TARGET_SCHEMA']

    project_db=args['PROJECT_DB']

    crc_schema = target_db + '.' + args['CRC_SCHEMA']
    hive_schema = target_db + '.' + args['HIVE_SCHEMA']
    pm_schema= target_db + '.' + args['PM_SCHEMA']
    metadata_schema= target_db + '.' + args['METADATA_SCHEMA'] 
    wd_schema = target_db + '.' + args['WORKDATA_SCHEMA']

    project_pm = project_db + '.' + args['PM_SCHEMA']
    project_hive =  project_db + '.' + args['HIVE_SCHEMA']
    stage_schema = target_db + '.' + 'enact_stage'

    kwargs = {
        'crc_schema': crc_schema,
        'hive_schema': hive_schema,
        'metadata_schema': metadata_schema,
        'pm_schema': pm_schema,
        'wd_schema': wd_schema,
        'stage_schema': stage_schema,
        'source_schema': source_schema,
        'target_schema': target_schema,
        'target_db': target_db,
        'project_pm': project_pm,
        'project_hive': project_hive,
    }

    # connect to snowflake
    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id, args]
    )


    # create i2b2 cell schema: i2b2data, i2b2metadata, i2b2hive, i2b2pm, i2b2workdata
    with TaskGroup('create-i2b2-schema') as init:
        CREATE_SCHEMA_PATH = f"{BASE_PATH}/CONFIGURE/COMMON/create_schema.sql"   
        read_sql = read_sql_from_file(CREATE_SCHEMA_PATH, **kwargs)
        create_tables_task = execute_sql(
            snowflake_conn_id, 
            get_task_id(CREATE_SCHEMA_PATH),
            read_sql
        )
        read_sql >> create_tables_task

    data_install_task = BashOperator(
        task_id='i2b2-data',
        bash_command=f"""
            {ANT_PATH} -f {ANT_BUILD_PATH} create_database load_demodata
        """,
        retries=0
    )

    
    with TaskGroup('i2b2-common-configure') as i2b2datacleanup:
        COMMON_CONFIGURE_PATH = f"{BASE_PATH}/CONFIGURE/COMMON/SERVICES"
        execute_sql_directory(
            snowflake_conn_id, 
            COMMON_CONFIGURE_PATH, 
            False, 
            **kwargs
        )       
    
    with TaskGroup('i2b2-project-configure') as i2b2projectconfigure:
        PROJECT_CONFIGURE_PATH = f"{BASE_PATH}/CONFIGURE/PROJECT/{project}"
        execute_sql_directory(
            snowflake_conn_id, 
            PROJECT_CONFIGURE_PATH, 
            False, 
            **kwargs
        )
    create_conn_task  >> init >> data_install_task >> i2b2datacleanup >> i2b2projectconfigure 


   