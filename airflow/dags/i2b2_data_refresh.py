from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values
from common import *

with DAG(
    "i2b2_data_refresh",
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
    description="pcornet to i2b2 data harmonization process",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["i2b2_data_refresh"],
) as dag:
    snowflake_conn_id = 'mu-dev'
    args = dotenv_values("/opt/airflow/env/dev/.env")
    project = args['PROJECT']

    BASE_PATH = '/opt/airflow/SCRIPTS/CDM_DATA'
    CROSS_WALK_PATH = f"{BASE_PATH}/CONFIGURE/{project}/crosswalk.sql"  
    DIMENSION_PATH = f"{BASE_PATH}/DIMENSION_TABLES/{project}"
    FACT_PATH = f"{BASE_PATH}/FACT_TABLES/{project}"
    GEN_COUNT_PATH = f"{BASE_PATH}/CDM_COUNT/generate_count.sql"
    MISSING_OBS_PATH = f"{BASE_PATH}/MISSING_OBS"
    PROJECT_CONFIG_PATH = f"{BASE_PATH}/CONFIGURE/{project}/project_config.sql"

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
   
    kwargs = {
        'metadata_schema': metadata_schema,
        'crc_schema': crc_schema,
        'source_schema': source_schema,
        'target_schema': target_schema,
        'pm_schema': pm_schema,
        'hive_schema': hive_schema,
        'project_pm': project_pm,
        'project_hive': project_hive
    }
    

    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id, args]
    )
   
    with TaskGroup('dimension_tables') as dimension_tables:
        execute_sql_directory(
            snowflake_conn_id, 
            DIMENSION_PATH, 
            False, 
            **kwargs
        )

    with TaskGroup('fact_tables') as fact_tables:
        execute_sql_directory(
            snowflake_conn_id, 
            FACT_PATH, 
            True,
            **kwargs
        )

    with TaskGroup('count_sql_task') as run_count_sql:
        sql_query = read_sql_from_file(
            GEN_COUNT_PATH, 
            **kwargs
        )
        count_sql_task = execute_sql(
            snowflake_conn_id, 
            get_task_id(GEN_COUNT_PATH), 
            sql_query,
            autocommit=True
        )
        sql_query >> count_sql_task
    
    with TaskGroup('missing_obs_tasks') as find_missing_obs:
        execute_sql_directory(
            snowflake_conn_id, 
            MISSING_OBS_PATH, 
            True,
            **kwargs
        )


    with TaskGroup('finalize') as project_config:
        read_sql = read_sql_from_file(
            PROJECT_CONFIG_PATH, 
            **kwargs
        )
        count_sql_task = execute_sql(
            snowflake_conn_id, 
            get_task_id(PROJECT_CONFIG_PATH), 
            read_sql
        )
        read_sql >> count_sql_task
        
    create_conn_task  >> dimension_tables >> fact_tables  >> project_config >> run_count_sql >> find_missing_obs

   