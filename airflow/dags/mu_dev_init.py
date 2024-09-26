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
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    "mu-dev-init",
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
    tags=["i2b2 data"],
) as dag:
    
    INIT_SCRIPT_PATH = '/opt/airflow/SCRIPTS/CDM_DATA/COMMON/INIT_CLEANUP/init.sql'
    ONT_BASE_PATH = '/opt/airflow/SCRIPTS/ONTOLOGY_TABLES'
    
    ENACT_PATH = f"{ONT_BASE_PATH}/ACT_V4_LOADER/ENACT_V41_POSTGRES_I2B2_TSV"
    
    LOCAL_STAGE=f"file://{ENACT_PATH}"
    
    DATA_INSTALLER_PATH = '/opt/airflow/SCRIPTS/i2b2-data'
    ANT_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/apache-ant/bin/ant"
    ANT_BUILD_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/NewInstall/build.xml"
    
    TSV_FORMAT = 'TSV_FORMAT'
    DSV_FORMAT = 'DSV_FORMAT'
    TSV_STAGE = 'i2b2_ont_import_tsv'
    DSV_STAGE = 'i2b2_ont_import_dsv'
    PUT_PARAMETERS="PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE"
    
    
    args = dotenv_values("/opt/airflow/env/sandbox/.env")
    
    snowflake_conn_id = 'mu-dev-init'
    project = 'mu'
    
    target_db='I2B2_ETL_TEST'
    target_schema = target_db + '.I2B2DATA'
    pm_db='I2B2_ETL_TEST'
    
    soruce_db='DEIDENTIFIED_PCORNET_CDM'
    source_schema = soruce_db + '.CDM'
    crc_schema = target_schema
    hive_schema = pm_db + '.I2B2HIVE' 
    metadata_schema= target_db + '.I2B2METADATA'
    pm_schema = pm_db + '.I2B2PM'
    wd_schema = target_db + '.I2B2WORKDATA'

    kwargs = {
        'crc_schema': crc_schema,
        'hive_schema': hive_schema,
        'metadata_schema': metadata_schema,
        'pm_schema': pm_schema,
        'wd_schema': wd_schema,
        'source_schema': source_schema,
        'target_schema': target_schema,
        'target_db': target_db
    }

    @provide_session
    def create_snowflake_connection(conn_id, session: Session =None):
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if existing_conn:
            print(f"Connection '{conn_id}' already exists.")
        else:
            con = connection.Connection(
                conn_id=conn_id,
                conn_type='snowflake',
                login=args['USERNAME'],
                password=args['PASSWORD'],
                schema=args['METADATA_SCHEMA'],
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
    
    # connect to snowflake
    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id]
    )

        # create i2b2 cell schemas
    with TaskGroup('init') as init:
        init_query_path = INIT_SCRIPT_PATH
        init_sql_query = read_sql_from_file(init_query_path, **kwargs)
        create_tables_task = execute_sql(get_task_id(init_query_path), init_sql_query)
        init_sql_query >> create_tables_task

    # Task to create multiple symlinks using BashOperator
    data_install_task = BashOperator(
        task_id='data_install_task',
        bash_command=f"""
            {ANT_PATH} -f {ANT_BUILD_PATH} create_database load_demodata
        """,
        retries=0
    )
   
    # create file formats and stages and upload to snowflake stge
    with TaskGroup('extract') as Extract:

        #tsv file format
        tsv_format_query = f"""
            CREATE OR REPLACE FILE FORMAT {TSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            ESCAPE=NONE
            NULL_IF = ('NULL')
            COMPRESSION=AUTO
            ESCAPE_UNENCLOSED_FIELD=NONE
            FIELD_OPTIONALLY_ENCLOSED_BY=NONE
            SKIP_HEADER=1;
        """
        
        tsv_format_task = SnowflakeOperator(
            task_id='tsv_format_task',
            snowflake_conn_id=snowflake_conn_id,
            sql=tsv_format_query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

        #dsv file format
        dsv_format_query = f"""
            CREATE OR REPLACE FILE FORMAT {DSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            COMPRESSION = AUTO
            SKIP_HEADER=1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        """
    
        dsv_format_task = SnowflakeOperator(
            task_id='dsv-format',
            snowflake_conn_id=snowflake_conn_id,
            sql=dsv_format_query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

        #snowflake stage
        create_stage_query_tsv = f"""
            CREATE OR REPLACE STAGE {TSV_STAGE} FILE_FORMAT = {TSV_FORMAT};
        """

        create_stage_query_dsv = f"""
            CREATE OR REPLACE STAGE {DSV_STAGE} FILE_FORMAT = {DSV_FORMAT};
        """

        create_tsv_stage_task = SnowflakeOperator(
            task_id='stage-tsv-create',
            snowflake_conn_id=snowflake_conn_id,
            sql=create_stage_query_tsv,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

        create_dsv_stage_task = SnowflakeOperator(
            task_id='stage-dsv-create',
            snowflake_conn_id=snowflake_conn_id,
            sql=create_stage_query_dsv,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )
        
        # upload tsvs to snwoflake stage
        stage_tsv_query = f"""
            PUT {LOCAL_STAGE}/*.tsv @{TSV_STAGE} {PUT_PARAMETERS};
        """

        stage_tsv_task = SnowflakeOperator(
            task_id='stage-tsv-upload',
            snowflake_conn_id=snowflake_conn_id,
            sql=stage_tsv_query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

        # upload dsvs to snwoflake stage
        stage_dsv_query = f"""
            PUT {LOCAL_STAGE}/*.dsv @{DSV_STAGE} {PUT_PARAMETERS};
        """

        stage_dsv_task = SnowflakeOperator(
            task_id='stage-dsv-upload',
            snowflake_conn_id=snowflake_conn_id,
            sql=stage_dsv_query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )
        format_empty = EmptyOperator(
            task_id='format-create'
        )
        stage_empty = EmptyOperator (
            task_id='stage-create'
        )

        [tsv_format_task, dsv_format_task] >> format_empty >> [create_tsv_stage_task, create_dsv_stage_task] >> stage_empty >> [stage_tsv_task, stage_dsv_task]
        
    
    # load staged data in the i2b2 cell schema
    with TaskGroup('load') as Load:

        with TaskGroup('ACT_DDL') as ACT_DDL:
            #create all i2b2 i2b2 ontology tables
            sql_file_path = f"{ENACT_PATH}/AA_CREATE_METADATA_TABLES_V41_POSTGRES.sql"
            sql_query = read_sql_from_file(sql_file_path, **kwargs)
            create_tables_task = execute_sql(get_task_id(sql_file_path), sql_query)
            sql_query >> create_tables_task

        # load staged data to i2b2 schema
        with TaskGroup('ACT_LOAD') as ACT_LOAD:
            # load_act_tables
            for file in sorted(os.listdir(ENACT_PATH)):
                filename = Path(file).stem
                filename_without_extension = filename.split(".")[0]

                if not filename.startswith('ACT'):
                    continue
                
                if filename_without_extension.endswith('PRECALC'):
                    continue
                
                if filename_without_extension.endswith('_POSTGRES'):
                    filename_without_extension = filename_without_extension.removesuffix('_POSTGRES')

                query = f"""
                    COPY INTO {filename_without_extension} FROM @{TSV_STAGE}/{filename} FILE_FORMAT={TSV_FORMAT};
                """

                load_act_tables = SnowflakeOperator(
                    task_id=f"load-{filename_without_extension}",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=query,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    retries=0
                )

        # schemes
        query = f"""
            COPY INTO SCHEMES FROM @{DSV_STAGE}/SCHEMES_V41.dsv FILE_FORMAT={DSV_FORMAT} ;
        """

        load_schemes_tasks = SnowflakeOperator(
            task_id='load-schemes',
            snowflake_conn_id=snowflake_conn_id,
            sql=query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

         # table access
        query = f"""
            COPY INTO TABLE_ACCESS FROM (
	            SELECT $1, $2, $3, $24, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
	            FROM @{DSV_STAGE}/TABLE_ACCESS_V41.dsv
            )
            FILE_FORMAT = {DSV_FORMAT};
        """

        load_ta_tasks = SnowflakeOperator(
            task_id='load-table-access',
            snowflake_conn_id=snowflake_conn_id,
            sql=query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

         # concepts
        query = f"""
            COPY INTO {crc_schema}.CONCEPT_DIMENSION FROM @{TSV_STAGE}/CONCEPT_DIMENSION_V41.tsv FILE_FORMAT={TSV_FORMAT} ;
        """

        load_concept_tasks = SnowflakeOperator(
            task_id='load-concepts',
            snowflake_conn_id=snowflake_conn_id,
            sql=query,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            retries=0
        )

        ACT_DDL >> ACT_LOAD >> [load_schemes_tasks, load_ta_tasks] >> load_concept_tasks



    with TaskGroup('transform') as Transform:
        
        sql_file_path = f"{ONT_BASE_PATH}/ACT_V4_LOADER/scripts/harmonize-proc.sql"
        sql_query = read_sql_from_file(sql_file_path, **kwargs)
        Harmonize_proc = execute_sql('act_harmonize_proc', sql_query)
        Harmonize_task =   SnowflakeOperator(
                    task_id='act_harmonize',
                    snowflake_conn_id=snowflake_conn_id,
                    sql="call harmonize_proc();",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0
                )
        sql_query >> Harmonize_proc >> Harmonize_task

    create_conn_task >> init >> data_install_task >> Extract >> Load >> Transform


   