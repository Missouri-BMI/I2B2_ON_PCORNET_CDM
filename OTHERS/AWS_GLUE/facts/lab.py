import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_connection = "snowflake-july22"
source_table = "DEID_LAB_FACT"

target_connection =  "postgres-glue-jdbc-connection"
target_table = "lab_fact"

source_data = (
        glueContext.create_dynamic_frame.from_options(
            connection_type="custom.jdbc",
            connection_options={
                "dbTable": source_table,
                "connectionName": source_connection,
                'hashexpression': 'TEXT_SEARCH_INDEX',
                'hashpartitions' : 100'
            }
        )
)


target_data = ApplyMapping.apply(
    frame=source_data,
    mappings=[("ENCOUNTER_NUM", "long", "encounter_num", "int"),
        ("PATIENT_NUM", "long", "patient_num", "int"),
        ("CONCEPT_CD", "string", "concept_cd", "string"),
        ("PROVIDER_ID", "string", "provider_id", "string"),
        ("START_DATE", "timestamp", "start_date", "timestamp"),
        ("MODIFIER_CD", "string", "modifier_cd", "string"),
        ("INSTANCE_NUM", "long", "instance_num", "int"),
        ("VALTYPE_CD", "string", "valtype_cd", "string"),
        ("TVAL_CHAR", "string", "tval_char", "string"),
        ("NVAL_NUM", "long", "nval_num", "long"),
        ("VALUEFLAG_CD", "string", "valueflag_cd", "string"),
        ("QUANTITY_NUM", "long", "quantity_num", "long"),
        ("UNITS_CD", "string", "units_cd", "string"),
        ("END_DATE", "timestamp", "end_date", "timestamp"),
        ("LOCATION_CD", "string", "location_cd", "string"),
        ("OBSERVATION_BLOB", "string", "observation_blob", "string"),
        ("CONFIDENCE_NUM", "long", "confidence_num", "long"),
        ("UPDATE_DATE", "timestamp", "update_date", "timestamp"),
        ("DOWNLOAD_DATE", "timestamp", "download_date", "timestamp"),
        ("IMPORT_DATE", "timestamp", "import_date", "timestamp"),
        ("SOURCESYSTEM_CD", "string", "sourcesystem_cd", "string"),
        ("UPLOAD_ID", "long", "upload_id", "int"),
        ("TEXT_SEARCH_INDEX", "long", "text_search_index", "int")
    ]
)


glueContext.write_dynamic_frame.from_options(
    frame=target_data,
    connection_type="custom.jdbc",
    connection_options={
        "dbTable": target_table,
        "connectionName": target_connection
    }
)
job.commit()

    


