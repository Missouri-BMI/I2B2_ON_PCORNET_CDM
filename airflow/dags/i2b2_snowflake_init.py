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
    "snowflake_i2b2",
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
    description="Export Snowflake enact ontology to i2b2-data",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["snowflake_i2b2"],
) as dag:
    
    snowflake_conn_id = 'mu-dev'
    args = dotenv_values("/opt/airflow/env/dev/.env")
    
    BASE_PATH = '/opt/airflow/SCRIPTS/DATA_INSTALLER'
    
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
    }


    create_conn_task = PythonOperator(
        task_id='connect',
        python_callable=create_snowflake_connection,
        op_args=[snowflake_conn_id, args]
    )
    
    create_stage = execute_sql(
            snowflake_conn_id, 
            'create-i2b2-data-stage-schema',
            f"CREATE OR REPLACE SCHEMA {stage_schema};"
        )

    with TaskGroup('enact-pcornet') as enact_pcornet:
        # create file formats and stages and upload enact concepts and metadata to snowflake stge
        with TaskGroup('extract') as Extract:
            STAGE_ACT_PATH = f"{BASE_PATH}/CONFIGURE/COMMON/stage_act.sql"
            read_sql = read_sql_from_file(STAGE_ACT_PATH, **kwargs)
            stage_act = execute_sql(snowflake_conn_id, 'stage-act-in-snowflake', read_sql)
            read_sql >> stage_act
        
        # load staged enact data in the i2b2 cell schema
        with TaskGroup('load') as Load:

            # create enact ontology tables
            with TaskGroup('ACT_DDL') as ACT_DDL:
                #create all enact i2b2 ontology tables in the stage schema
                ENACT_DDL = f"{ENACT_DATA}/AA_CREATE_METADATA_TABLES_V41_POSTGRES.sql"
            
                read_sql = read_sql_from_file(ENACT_DDL, **kwargs)
                final_sql =add_schema_sql(stage_schema, read_sql)
                
                create_tables_task = execute_sql(
                    snowflake_conn_id, 
                    get_task_id(ENACT_DDL),
                    final_sql
                )
                read_sql >> final_sql >> create_tables_task

            # load staged ontologoy data to i2b2 metadata schema
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
                        COPY INTO {stage_schema}.{filename_without_extension} FROM @{stage_schema}.{TSV_STAGE}/{filename} FILE_FORMAT={stage_schema}.{TSV_FORMAT};
                    """

                    load_act_tables = execute_sql(
                        snowflake_conn_id, 
                        f"load-{filename_without_extension}",
                        query
                    )

            # load ontology schemes
            query = f"""
                CREATE TABLE {stage_schema}.SCHEMES 
                (	C_KEY VARCHAR(50)	 	NOT NULL,
                    C_NAME VARCHAR(50)		NOT NULL,
                    C_DESCRIPTION VARCHAR(100)	NULL
                ) ;
            
                COPY INTO {stage_schema}.SCHEMES FROM @{stage_schema}.{DSV_STAGE}/SCHEMES_V41.dsv FILE_FORMAT={stage_schema}.{DSV_FORMAT} ;

            """

            load_schemes_tasks = execute_sql(
                snowflake_conn_id, 
                'load-schemes',
                query
            )

            # load table access
            query = f"""
                CREATE TABLE {stage_schema}.TABLE_ACCESS
                (	C_TABLE_CD VARCHAR(50)	NOT NULL, 
                    C_TABLE_NAME VARCHAR(50)	NOT NULL, 
                    C_PROTECTED_ACCESS CHAR(1)	NULL,
                    C_ONTOLOGY_PROTECTION	TEXT	NULL,
                    C_HLEVEL INT				NOT NULL, 
                    C_FULLNAME VARCHAR(700)	NOT NULL, 
                    C_NAME VARCHAR(2000)		NOT NULL, 
                    C_SYNONYM_CD CHAR(1)	NOT NULL, 
                    C_VISUALATTRIBUTES CHAR(3)	NOT NULL, 
                    C_TOTALNUM INT			NULL, 
                    C_BASECODE VARCHAR(50)	NULL, 
                    C_METADATAXML TEXT		NULL, 
                    C_FACTTABLECOLUMN VARCHAR(50)	NOT NULL, 
                    C_DIMTABLENAME VARCHAR(50)	NOT NULL, 
                    C_COLUMNNAME VARCHAR(50)	NOT NULL, 
                    C_COLUMNDATATYPE VARCHAR(50)	NOT NULL, 
                    C_OPERATOR VARCHAR(10)	NOT NULL, 
                    C_DIMCODE VARCHAR(700)	NOT NULL, 
                    C_COMMENT TEXT	NULL, 
                    C_TOOLTIP VARCHAR(900)	NULL, 
                    C_ENTRY_DATE timestamp		NULL, 
                    C_CHANGE_DATE timestamp	NULL, 
                    C_STATUS_CD CHAR(1)		NULL,
                    VALUETYPE_CD VARCHAR(50)	NULL
                ) 
                ;

                COPY into {stage_schema}.TABLE_ACCESS FROM (
                    SELECT $1, $2, $3, $24, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
                    FROM @{stage_schema}.{DSV_STAGE}/TABLE_ACCESS_V41.dsv
                )
                FILE_FORMAT = {stage_schema}.{DSV_FORMAT};
            """

            load_ta_tasks = execute_sql(
                snowflake_conn_id, 
                'load-table-access',
                query
            )

            # load concept_dimension
            query = f"""
                CREATE TABLE {stage_schema}.CONCEPT_DIMENSION ( 
                    CONCEPT_PATH   		VARCHAR(700) NOT NULL,
                    CONCEPT_CD     		VARCHAR(50) NULL,
                    NAME_CHAR      		VARCHAR(2000) NULL,
                    CONCEPT_BLOB   		TEXT NULL,
                    UPDATE_DATE    		TIMESTAMP NULL,
                    DOWNLOAD_DATE  		TIMESTAMP NULL,
                    IMPORT_DATE    		TIMESTAMP NULL,
                    SOURCESYSTEM_CD		VARCHAR(50) NULL,
                    UPLOAD_ID			INT NULL
                    )
                ;
                COPY into {stage_schema}.CONCEPT_DIMENSION 
                    FROM @{stage_schema}.{TSV_STAGE}/CONCEPT_DIMENSION_V41.tsv FILE_FORMAT={stage_schema}.{TSV_FORMAT} ;
            """

            load_concept_tasks = execute_sql(
                snowflake_conn_id, 
                'load-concepts',
                query
            )
            ACT_DDL >> ACT_LOAD >> [load_schemes_tasks, load_ta_tasks, load_concept_tasks]

        # convert postgresql queries to snowflake queries, multi-fact mappings for pconrnet to enact i2b2
        with TaskGroup('transform') as Transform:
            HARMONIZE_PATH = f"{ENACT_PATH}/scripts/harmonize-proc.sql"
            sql_read = read_sql_from_file(HARMONIZE_PATH, **kwargs)
            
            Harmonize_proc = execute_sql(
                snowflake_conn_id, 
                'act_harmonize_proc', 
                sql_read
            )
            Harmonize_task = execute_sql(
                snowflake_conn_id, 
                'act_harmonize',
                f"USE SCHEMA {stage_schema}; call harmonize_proc();", 
                autocommit=True
            )

            sql_read >> Harmonize_proc >> Harmonize_task
            
        Extract >> Load >> Transform    

    with TaskGroup('i2b2-data-export') as i2b2_data_export_task:
        #stage all enact i2b2 ontology tables to snowflake stage
        with TaskGroup('stage-table') as stage_table_task:
            sql_file = f"{BASE_PATH}/CONFIGURE/COMMON/extract_concept.sql"
            read_sql = read_sql_from_file(sql_file, **kwargs)
            create_tables_task = execute_sql(
                snowflake_conn_id, 
                'concept-dimension-export',
                read_sql
            )
            read_sql >> create_tables_task

        with TaskGroup('export-concepts') as export_concepts_task:
            CRC_CONCEPT_PATH = f"{BASE_PATH}/i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/Crcdata/act/scripts/snowflake"
            CONCEPT_EXPORT_PATH=f"file://{CRC_CONCEPT_PATH}"
            download_concept = execute_sql(
                snowflake_conn_id, 
                'concept-dimension-export',
                 f"GET @{stage_schema}.CSV_STAGE/CONCEPT_DIMENSION.csv {CONCEPT_EXPORT_PATH} OVERWRITE=TRUE"
            )
             # Task to zip files
            zip_files = BashOperator(
                task_id='zip_concept_files',
                bash_command=f"""
                i=1;
                for file in {CRC_CONCEPT_PATH}/*.csv.gz; do
                    zip -j {CRC_CONCEPT_PATH}/crcdata${{i}}.zip "$file";
                    rm "$file";
                    i=$((i+1));
                done
                """,
                retries=0
            )
            download_concept >> zip_files
        
        with TaskGroup('export-metadata') as export_metadata_task:
            ONT_PATH = f"{BASE_PATH}/i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/Metadata/act/scripts/snowflake"
            ONT_EXPORT_PATH=f"file://{ONT_PATH}"
            download_ont = execute_sql(
                snowflake_conn_id, 
                'act-metadata-export',
                 f"GET @{stage_schema}.CSV_STAGE {ONT_EXPORT_PATH} PATTERN='.*\.csv.gz' OVERWRITE=TRUE"
            )

             # Task to zip files
            zip_files = BashOperator(
                task_id='zip_metadata_files',
                bash_command=f"""
                i=1;
                for file in {ONT_PATH}/*.csv.gz; do
                    if [[ "$(basename "$file")" == CONCEPT_DIMENSION* ]]; then
                        rm "$file";
                    else
                        zip -j {ONT_PATH}/metadata${{i}}.zip "$file";
                        rm "$file";
                        i=$((i+1));
                    fi
                done
                """,
                retries=0
            )
            download_ont  >> zip_files
            
        stage_table_task >> export_concepts_task >> export_metadata_task
                
    create_conn_task >> create_stage >> enact_pcornet >> i2b2_data_export_task