from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values
from common import *

with DAG(
    "test_dag",
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
    description="test dag",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["test_dag"],
) as dag:
    
    snowflake_conn_id = 'mu-dev'
    args = dotenv_values("/opt/airflow/env/dev/.env")
    
    BASE_PATH = '/opt/airflow/SCRIPTS/DATA_INSTALLER'
    CRC_CONCEPT_PATH = f"{BASE_PATH}/i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/Crcdata/act/scripts/snowflake"
    CONCEPT_EXPORT_PATH=f"file://{CRC_CONCEPT_PATH}"
    
    TSV_FORMAT = 'TSV_FORMAT'
    TSV_STAGE = 'i2b2_ont_import_tsv'
    DSV_FORMAT = 'DSV_FORMAT'
    DSV_STAGE = 'i2b2_ont_import_dsv'

    ENACT_PATH = f"{BASE_PATH}/ACT_V4_LOADER"
    ENACT_DATA = f"{ENACT_PATH}/ENACT_V41_POSTGRES_I2B2_TSV"
 
    LOCAL_STAGE=f"file://{ENACT_DATA}"
    PUT_PARAMETERS="PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE"
    
    
    target_db= args['TARGET_DB']
    target_schema = target_db + '.' + args['TARGET_SCHEMA']

    crc_schema = target_db + '.' + args['CRC_SCHEMA']
    hive_schema = target_db + '.' + args['HIVE_SCHEMA']
    pm_schema= target_db + '.' + args['PM_SCHEMA']
    metadata_schema= target_db + '.' + args['METADATA_SCHEMA'] 
    wd_schema = target_db + '.' + args['WORKDATA_SCHEMA']
    stage_schema = target_db + '.' + 'enact_stage'

    kwargs = {
        'crc_schema': crc_schema,
        'hive_schema': hive_schema,
        'metadata_schema': metadata_schema,
        'pm_schema': pm_schema,
        'wd_schema': wd_schema,
        'target_schema': target_schema,
        'stage_schema': stage_schema,
        'target_db': target_db,
        'TSV_FORMAT': TSV_FORMAT,
        'TSV_STAGE': TSV_STAGE,    
        'DSV_FORMAT': DSV_FORMAT,
        'DSV_STAGE': DSV_STAGE,
        'LOCAL_STAGE': LOCAL_STAGE,
        'PUT_PARAMETERS': PUT_PARAMETERS,
        'EXPORT_PATH': CONCEPT_EXPORT_PATH,
    }

    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id, args]
    )