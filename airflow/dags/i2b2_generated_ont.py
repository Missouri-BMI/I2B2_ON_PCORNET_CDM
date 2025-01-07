from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values
from common import *

with DAG(
    "i2b2_generated_ontology",
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
    description="generate non-ontology in i2b2",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["i2b2_custom_ontology"],
) as dag:
    snowflake_conn_id = 'mu-dev'
    args = dotenv_values("/opt/airflow/env/sandbox/.env")
    project = args['PROJECT']

    BASE_PATH = '/opt/airflow/SCRIPTS/DATA_INSTALLER/GENERATED_ONT'
    ACS_SCRIPT = f"{BASE_PATH}/ACS/build.sql"
    ADI_SCRIPT = f"{BASE_PATH}/ADI_RANKING/build.sql"
    FACILITY_ONTOLOGY_PATH = f"{BASE_PATH}/FACILITY_ONTOLOGY"
    LOINC_DOC_PATH = f"{BASE_PATH}/LOINC_DOC_ONTOLOGY"
    MEDLIST_SCRIPT = f"{BASE_PATH}/MED_LIST/build.sql"
    NAACCR_PATH = f"{BASE_PATH}/NAACCR_SNOWFLAKE"
    SITES = f"{BASE_PATH}/SITES"

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


    with TaskGroup('ACS') as acs:
        sql_query = read_sql_from_file(
            ACS_SCRIPT, 
            **kwargs
        )

        acs_task = execute_sql(
            snowflake_conn_id, 
            'build-acs', 
            sql_query
            
        )       
        sql_query >> acs_task

    with TaskGroup('ADI') as adi:
        sql_query = read_sql_from_file(
            ADI_SCRIPT, 
            **kwargs
        )

        adi_task = execute_sql(
            snowflake_conn_id, 
            'build-adi', 
            sql_query
            
        )       
        sql_query >> adi_task

    with TaskGroup('MEDLIST') as medlist:
        sql_query = read_sql_from_file(
            MEDLIST_SCRIPT, 
            **kwargs
        )

        medlist_task = execute_sql(
            snowflake_conn_id, 
            'build-medlist', 
            sql_query
        )       
        sql_query >> medlist_task
    
    with TaskGroup('Facility-id') as facilityID:
        FACILITY_ID_PATH = f"{FACILITY_ONTOLOGY_PATH}/facility_id.sql"
        sql_query = read_sql_from_file(
            FACILITY_ID_PATH, 
            **kwargs
        )

        facility_id_task = execute_sql(
            snowflake_conn_id, 
            'build-facility-id', 
            sql_query
            
        )       
        sql_query >> facility_id_task
    
    with TaskGroup('Facility-location') as facilityLocation:
        FACILITY_LOCATION_PATH = f"{FACILITY_ONTOLOGY_PATH}/facility_location.sql"
        sql_query = read_sql_from_file(
            FACILITY_LOCATION_PATH, 
            **kwargs
        )

        facility_location_task = execute_sql(
            snowflake_conn_id, 
            'build-facility_location', 
            sql_query
            
        )       
        sql_query >> facility_location_task
    
    with TaskGroup('SITES') as sites:
        LOCAL_STAGE=f"file://{SITES}/{project}"
        TSV_STAGE = 'i2b2_ont_import_tsv'
        PUT_PARAMETERS="PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE"
        TSV_FORMAT = 'TSV_FORMAT'

        stage_tsv_query = f"""
            USE SCHEMA {metadata_schema};
            PUT {LOCAL_STAGE}/*.tsv @{TSV_STAGE} {PUT_PARAMETERS};
        """.format(**kwargs)

        stage_tsv_task = execute_sql(
            snowflake_conn_id, 
            'sites-tsv-upload',
            stage_tsv_query
        )

        query = f"""
            USE SCHEMA {metadata_schema};
            DELETE FROM ACT_DEM_V41 WHERE C_FULLNAME LIKE '%\\GPC Sites\\%' or C_FULLNAME LIKE '%\\PCORNet Sites\\%';
            COPY INTO ACT_DEM_V41 FROM @{TSV_STAGE}/sites.tsv FILE_FORMAT={TSV_FORMAT} ;
        """.format(**kwargs)

        load_sites_tasks = execute_sql(
            snowflake_conn_id, 
            'load-sites',
            query
        )

        stage_done = EmptyOperator(
            task_id='site_stage_tasks_ends', 
            trigger_rule=TriggerRule.ALL_DONE
        )
        stage_tsv_task >> load_sites_tasks >> stage_done


    create_conn_task >> sites >> facilityID >> facilityLocation >> adi >> medlist >> acs

   