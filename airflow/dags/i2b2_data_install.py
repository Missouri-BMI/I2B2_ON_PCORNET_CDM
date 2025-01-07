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
    
    CREATE_SCHEMA_PATH = f"{BASE_PATH}/CONFIGURE/COMMON/create_schema.sql"
    COMMON_CONFIGURE_PATH = f"{BASE_PATH}/CONFIGURE/COMMON/SERVICES"
    PROJECT_CONFIGURE_PATH = f"{BASE_PATH}/CONFIGURE/PROJECT/{project}"
    
    ENACT_PATH = f"{BASE_PATH}/ACT_V4_LOADER"
    ENACT_DATA = f"{ENACT_PATH}/ENACT_V41_POSTGRES_I2B2_TSV"
    HARMONIZE_PATH = f"{ENACT_PATH}/scripts/harmonize-proc.sql"
    ENACT_DDL = f"{ENACT_DATA}/AA_CREATE_METADATA_TABLES_V41_POSTGRES.sql"
    LOCAL_STAGE=f"file://{ENACT_DATA}"
    
    DATA_INSTALLER_PATH = f"{BASE_PATH}/i2b2-data"
    ANT_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/apache-ant/bin/ant"
    ANT_BUILD_PATH=f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/NewInstall/build.xml"

    TSV_FORMAT = 'TSV_FORMAT'
    DSV_FORMAT = 'DSV_FORMAT'
    TSV_STAGE = 'i2b2_ont_import_tsv'
    DSV_STAGE = 'i2b2_ont_import_dsv'
    PUT_PARAMETERS="PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE"
    
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
        'crc_schema': crc_schema,
        'hive_schema': hive_schema,
        'metadata_schema': metadata_schema,
        'pm_schema': pm_schema,
        'wd_schema': wd_schema,
        'source_schema': source_schema,
        'target_schema': target_schema,
        'target_db': target_db,
        'project_pm': project_pm,
        'project_hive': project_hive
    }

    # connect to snowflake
    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id, args]
    )

        # create i2b2 cell schemas
    with TaskGroup('create-i2b2-schema') as init:
        read_sql = read_sql_from_file(CREATE_SCHEMA_PATH, **kwargs)
        create_tables_task = execute_sql(
            snowflake_conn_id, 
            get_task_id(CREATE_SCHEMA_PATH),
            read_sql
        )
        read_sql >> create_tables_task

    # Task to create multiple symlinks using BashOperator
    with TaskGroup('i2b2-data') as i2b2data:
        data_install_task = BashOperator(
            task_id='data_install_task',
            bash_command=f"""
                {ANT_PATH} -f {ANT_BUILD_PATH} create_database load_demodata
            """,
            retries=0
        )

        with TaskGroup('i2b2-common-configure') as i2b2datacleanup:
            execute_sql_directory(
                snowflake_conn_id, 
                COMMON_CONFIGURE_PATH, 
                False, 
                **kwargs
            )       
        
        with TaskGroup('i2b2-project-configure') as i2b2projectconfigure:
            execute_sql_directory(
                snowflake_conn_id, 
                PROJECT_CONFIGURE_PATH, 
                False, 
                **kwargs
            )

        data_install_task >> i2b2datacleanup >> i2b2projectconfigure

    # create file formats and stages and upload to snowflake stge
    with TaskGroup('extract') as Extract:

        #tsv file format
        tsv_format_query = f"""
            CREATE OR REPLACE FILE FORMAT {metadata_schema}.{TSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            ESCAPE=NONE
            NULL_IF = ('NULL')
            COMPRESSION=AUTO
            ESCAPE_UNENCLOSED_FIELD=NONE
            FIELD_OPTIONALLY_ENCLOSED_BY=NONE
            SKIP_HEADER=1;
        """
        
        tsv_format_task = execute_sql(
            snowflake_conn_id, 
            'tsv_format_task',
            tsv_format_query
        )
       

        #dsv file format
        dsv_format_query = f"""
            CREATE OR REPLACE FILE FORMAT {metadata_schema}.{DSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            COMPRESSION = AUTO
            SKIP_HEADER=1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        """
    
        dsv_format_task = execute_sql(
            snowflake_conn_id, 
            'dsv_format_task',
            dsv_format_query
        )

        

        #snowflake stage
        create_stage_query_tsv = f"""
            CREATE OR REPLACE STAGE {metadata_schema}.{TSV_STAGE} FILE_FORMAT = {metadata_schema}.{TSV_FORMAT};
        """

        create_stage_query_dsv = f"""
            CREATE OR REPLACE STAGE {metadata_schema}.{DSV_STAGE} FILE_FORMAT = {metadata_schema}.{DSV_FORMAT};
        """

        create_tsv_stage_task = execute_sql(
            snowflake_conn_id, 
            'stage-tsv-create',
            create_stage_query_tsv
        )
        

        create_dsv_stage_task = execute_sql(
            snowflake_conn_id, 
            'stage-dsv-create',
            create_stage_query_dsv
        )
        
        # upload tsvs to snwoflake stage
        stage_tsv_query = f"""
            PUT {LOCAL_STAGE}/*.tsv @{metadata_schema}.{TSV_STAGE} {PUT_PARAMETERS};
        """

        stage_tsv_task = execute_sql(
            snowflake_conn_id, 
            'stage-tsv-upload',
            stage_tsv_query
        )
         
        # upload dsvs to snwoflake stage
        stage_dsv_query = f"""
            PUT {LOCAL_STAGE}/*.dsv @{metadata_schema}.{DSV_STAGE} {PUT_PARAMETERS};
        """

        stage_dsv_task = execute_sql(
            snowflake_conn_id, 
            'stage-dsv-upload',
            stage_dsv_query
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
            read_sql = read_sql_from_file(ENACT_DDL, **kwargs)
            create_tables_task = execute_sql(
                snowflake_conn_id, 
                get_task_id(ENACT_DDL),
                read_sql
            )
            read_sql >> create_tables_task

        # load staged data to i2b2 schema
        with TaskGroup('ACT_LOAD') as ACT_LOAD:
            # load_act_tables
            for file in sorted(os.listdir(ENACT_DATA)):
                filename = Path(file).stem
                filename_without_extension = filename.split(".")[0]

                if not filename.startswith('ACT'):
                    continue
                
                if filename_without_extension.endswith('PRECALC'):
                    continue
                
                if filename_without_extension.endswith('_POSTGRES'):
                    filename_without_extension = filename_without_extension.removesuffix('_POSTGRES')

                query = f"""
                    COPY INTO {metadata_schema}.{filename_without_extension} FROM @{metadata_schema}.{TSV_STAGE}/{filename} FILE_FORMAT={metadata_schema}.{TSV_FORMAT};
                """

                load_act_tables = execute_sql(
                    snowflake_conn_id, 
                    f"load-{filename_without_extension}",
                    query
                )

        # schemes
        query = f"""
            COPY INTO {metadata_schema}.SCHEMES FROM @{metadata_schema}.{DSV_STAGE}/SCHEMES_V41.dsv FILE_FORMAT={metadata_schema}.{DSV_FORMAT} ;
        """

        load_schemes_tasks = execute_sql(
            snowflake_conn_id, 
            'load-schemes',
            query
        )

         # table access
        query = f"""
            COPY INTO {metadata_schema}.TABLE_ACCESS FROM (
	            SELECT $1, $2, $3, $24, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
	            FROM @{metadata_schema}.{DSV_STAGE}/TABLE_ACCESS_V41.dsv
            )
            FILE_FORMAT = {metadata_schema}.{DSV_FORMAT};
        """

        load_ta_tasks = execute_sql(
            snowflake_conn_id, 
            'load-table-access',
            query
        )

         # concepts
        query = f"""
            COPY INTO {crc_schema}.CONCEPT_DIMENSION FROM @{metadata_schema}.{TSV_STAGE}/CONCEPT_DIMENSION_V41.tsv FILE_FORMAT={metadata_schema}.{TSV_FORMAT} ;
        """

        load_concept_tasks = execute_sql(
            snowflake_conn_id, 
            'load-concepts',
            query
        )

        ACT_DDL >> ACT_LOAD >> [load_schemes_tasks, load_ta_tasks] >> load_concept_tasks



    with TaskGroup('transform') as Transform:
        sql_read = read_sql_from_file(HARMONIZE_PATH, **kwargs)
        Harmonize_proc = execute_sql(
            snowflake_conn_id, 
            'act_harmonize_proc', 
            sql_read
        )
        Harmonize_task = execute_sql(
            snowflake_conn_id, 
            'act_harmonize',
            "call harmonize_proc();", 
            autocommit=True
        )
        sql_read >> Harmonize_proc >> Harmonize_task

    create_conn_task >> init >> i2b2data >> Extract >> Load >> Transform


   