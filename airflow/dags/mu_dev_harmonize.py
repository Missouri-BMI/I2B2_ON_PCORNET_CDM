from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.utils.session import provide_session
from airflow.models import connection
from sqlalchemy.orm import Session
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from pathlib import Path
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
import sqlparse
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values

@provide_session
def create_snowflake_connection(conn_id, session: Session =None):

    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        print(f"Connection '{conn_id}' already exists.")
    else:
        args = dotenv_values("/opt/airflow/env/sandbox/.env")
        con = connection.Connection(
            conn_id=conn_id,
            conn_type='snowflake',
            login=args['USERNAME'],
            password=args['PASSWORD'],
            # schema=args['METADATA_SCHEMA'],
            extra=f"""{{
                "account": "{args['ACCOUNT']}",
                "database": "{args['TARGET_DB']}",
                "warehouse": "{args['WAREHOUSE']}",
                "role": "{args['ROLE']}"
            }}"""    
        )
        session.add(con)
        session.commit()
        print(f"Connection '{conn_id}' created successfully.")

with DAG(
    "mu-dev-harmonization",
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
    tags=["harmonizer"],
) as dag:
    
    BASE_PATH = '/opt/airflow/SCRIPTS/CDM_DATA'
    
    snowflake_conn_id = 'mu-dev'
    project = 'mu'
    
    soruce_db='DEIDENTIFIED_PCORNET_CDM'
    source_schema = soruce_db + '.CDM'

    target_db='I2B2_ETL_TEST'
    target_schema = target_db + '.I2B2DATA'
    
    pm_db='I2B2_ETL_TEST'
    pm_schema = pm_db + '.I2B2PM'

    crc_schema = target_schema
    metadata_schema= target_db + '.I2B2METADATA'
    hive_schema = pm_db + '.I2B2HIVE' 
   
    kwargs = {
        'metadata_schema': metadata_schema,
        'crc_schema': crc_schema,
        'source_schema': source_schema,
        'target_schema': target_schema,
        'pm_schema': pm_schema,
        'hive_schema': hive_schema
    }
    
    @task(retries=0)
    def read_sql_from_file(file_path: str, **kwargs) -> str:
        # Read SQL file from the specified directory
        sql_path = Path(file_path)
        if not sql_path.is_file():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        # Read the content of the SQL file
        with open(sql_path, 'r') as sql_file:
            sql_content = sql_file.read()

         # If the file is empty, return None
        if not sql_content.strip():
            raise ValueError(f"SQL file {file_path} is empty")
            
        sqlquery = sqlparse.format(sql_content, reindent=True, keyword_case='upper')
        return sqlquery.format(**kwargs)
    
    # Task to execute SQL using SnowflakeOperator
    def execute_sql(task_id, sql_query: str, trigger_rule=TriggerRule.ALL_SUCCESS):
        return SnowflakeOperator(
            task_id=task_id,
            snowflake_conn_id=snowflake_conn_id,
            sql=sql_query,
            trigger_rule=trigger_rule,
            retries=0
        )

    def get_task_id(file_path):
        filename = Path(file_path).stem
        return f'execute_{filename.split(".")[0]}'   
    
    def execute_sql_directory(sql_directory: str, sequentially: bool, **kwargs) -> str:
        tasks = []
        for filename in sorted(os.listdir(sql_directory)):
            if filename.endswith('.sql'):

                with TaskGroup(Path(filename).stem) as sub_group:
                    sql_file_path = os.path.join(sql_directory, filename)
                    read_sql = read_sql_from_file(sql_file_path, **kwargs)
                    sql_task = execute_sql(get_task_id(sql_file_path), read_sql)
                    read_sql >> sql_task
                
                tasks.append(sub_group)
     
        if sequentially:
            for i in range(len(tasks) - 1):
                tasks[i] >> tasks[i + 1]

    
    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id]
    )

    with TaskGroup('init_cleanup') as group_0:
        sql_file_path = f"{BASE_PATH}/COMMON/INIT_CLEANUP/clean_proc.sql"
        
        read_sql = read_sql_from_file(sql_file_path, **kwargs)
        clean_proc_task = execute_sql(get_task_id(sql_file_path), read_sql)

        read_sql >> clean_proc_task

    with TaskGroup('crosswalk') as group_1:
        sql_file_path = f"{BASE_PATH}/OTHER/{project}/crosswalk.sql"  
        read_sql = read_sql_from_file(sql_file_path, **kwargs)
        
        crosswalk_task_id = get_task_id(sql_file_path)
        crosswalk_tasks = execute_sql(crosswalk_task_id, read_sql,trigger_rule=TriggerRule.ALL_SUCCESS)       
        success = EmptyOperator(task_id='crosswalk_tasks_ends', trigger_rule=TriggerRule.ALWAYS)
        read_sql  >> crosswalk_tasks >> success
    
    with TaskGroup('dimension_tables') as group_2:
        sql_directory = f"{BASE_PATH}/DIMENSION_TABLES/{project}"
        execute_sql_directory(sql_directory, False, **kwargs)

    with TaskGroup('fact_tables') as group_3:
        sql_directory = f"{BASE_PATH}/FACT_TABLES/{project}"
        execute_sql_directory(sql_directory, True, **kwargs)

    with TaskGroup('count_sql_task') as group_4:
        sql_file_path =  f"{BASE_PATH}/COMMON/CDM_COUNT/generate_count.sql"
        read_sql = read_sql_from_file(sql_file_path, **kwargs)
        count_sql_task = execute_sql(get_task_id(sql_file_path), read_sql)
        read_sql >> count_sql_task

    
    with TaskGroup('missing_obs_tasks') as group_5:
        sql_directory = f"{BASE_PATH}/COMMON/MISSING_OBS"
        execute_sql_directory(sql_directory, True, **kwargs)


    with TaskGroup('finalize') as group_6:
        sql_file_path = f"{BASE_PATH}/OTHER/{project}/project_config.sql"
        read_sql = read_sql_from_file(sql_file_path, **kwargs)
        count_sql_task = execute_sql(get_task_id(sql_file_path), read_sql)
        read_sql >> count_sql_task
        
    create_conn_task >> group_0 >> group_1 >> group_2 >> group_3 >> group_4 >> group_5 >> group_6

   