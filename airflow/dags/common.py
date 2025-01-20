from airflow.utils.session import provide_session
from airflow.models import connection
from sqlalchemy.orm import Session
from airflow.models import Connection
from pathlib import Path
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
import sqlparse
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup


@provide_session
def create_snowflake_connection(conn_id, conn_params, session: Session =None):
    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        print(f"Connection '{conn_id}' already exists.")
    else:
        con = connection.Connection(
            conn_id=conn_id,
            conn_type='snowflake',
            login=conn_params['USERNAME'],
            password=conn_params['PASSWORD'],
            schema=conn_params['METADATA_SCHEMA'],
            extra=f"""{{
                "account": "{conn_params['ACCOUNT']}",
                "database": "{conn_params['TARGET_DB']}",
                "warehouse": "{conn_params['WAREHOUSE']}",
                "role": "{conn_params['ROLE']}"
            }}"""    
        )
        session.add(con)
        session.commit()
        print(f"Connection '{conn_id}' created successfully.")

@task(retries=0)
def read_sql_from_file(file_path: str, **kwargs) -> str:
    # Read SQL file from the specified directory
    sql_path = Path(file_path)
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    # Read the content of the SQL file
    with open(sql_path, 'r') as sql_file:
        sql_content = sql_file.read()

    if not sql_content.strip():
        raise ValueError(f"SQL file {file_path} is empty")
        
    # Format the SQL query and handle any dynamic content if necessary
    formatted_sql = sqlparse.format(
        sql_content, 
        reindent=False
    )
    return formatted_sql.format(**kwargs)

@task(retries=0)  
def add_schema_sql(schema, sql_text) -> str:
    return f"use schema {schema};\n" + sql_text
    
# Task to execute SQL using SnowflakeOperator
def execute_sql(conn_id, task_id, sql_query: str, trigger_rule=TriggerRule.ALL_SUCCESS, autocommit = True, retries = 0):
    return SnowflakeOperator(
        task_id=task_id,
        snowflake_conn_id=conn_id,
        sql=sql_query,
        trigger_rule=trigger_rule,
        autocommit=autocommit,
        retries=retries
    )



def get_task_id(file_path):
    filename = Path(file_path).stem
    return f'execute_{filename.split(".")[0]}'   

def execute_sql_directory(conn_id, sql_directory: str, sequentially: bool, **kwargs) -> str:
    tasks = []
    for filename in sorted(os.listdir(sql_directory)):
        ##TODO: revert it back
        if filename.endswith('.sql') and not filename.startswith('12_acs_fact'):
            with TaskGroup(Path(filename).stem) as sub_group:
                sql_file_path = os.path.join(sql_directory, filename)
                read_sql = read_sql_from_file(sql_file_path, **kwargs)
                sql_task = execute_sql(conn_id, get_task_id(sql_file_path), read_sql)
                read_sql >> sql_task
            
            tasks.append(sub_group)

    if sequentially:
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

