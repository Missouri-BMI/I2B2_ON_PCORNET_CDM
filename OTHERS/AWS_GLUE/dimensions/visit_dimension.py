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

# Script generated for node snowflake-jdbc-connector
source_data = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="custom.jdbc",
        connection_options={
            "dbTable": "DEID_VISIT_DIMENSION",
            "connectionName": "snowflake-july22",
            'hashfield': 'ENCOUNTER_NUM'
        }
    )
)

# Script generated for node Apply Mapping
target_data = ApplyMapping.apply(
    frame=source_data,
    mappings=[("ENCOUNTER_NUM", "long", "encounter_num", "int"),
         ("PATIENT_NUM", "long", "patient_num", "int"),
         ("PROVIDER_ID", "string", "provider_id", "string"),
         ("START_DATE", "timestamp", "start_date", "date"),
         ("END_DATE", "timestamp", "end_date", "date"),
         ("ENC_TYPE", "string", "enc_type", "string"),
         ("LENGTH_OF_STAY", "long", "length_of_stay", "int"),
         ("UPDATE_DATE", "timestamp", "update_date", "date"),
         ("DOWNLOAD_DATE", "timestamp", "download_date", "date"),
         ("IMPORT_DATE", "timestamp", "import_date", "date"),
         ("SOURCESYSTEM_CD", "string", "sourcesystem_cd", "string"),
         ("UPLOAD_ID", "long", "upload_id", "int")]
)

# Script generated for node postgres-jdbc-connector
glueContext.write_dynamic_frame.from_options(
    frame=target_data,
    connection_type="custom.jdbc",
    connection_options={
        "dbTable": "visit_dimension",
        "connectionName": "postgres-glue-jdbc-connection"
    }
)

job.commit()
