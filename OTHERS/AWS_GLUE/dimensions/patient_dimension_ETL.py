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
            "dbTable": "DEID_PATIENT_DIMENSION",
            "connectionName": "snowflake-july22",
            'hashfield': 'PATIENT_NUM'
        }
    )
)

# Script generated for node Apply Mapping
target_data = ApplyMapping.apply(
    frame=source_data,
    mappings=[("PATIENT_NUM", "long", "patient_num", "int"),
         ("VITAL_STATUS_CD", "string", "vital_status_cd", "string"),
         ("BIRTH_DATE", "timestamp", "birth_date", "date"),
         ("DEATH_DATE", "timestamp", "death_date", "date"),
         ("SEX_CD", "string", "sex_cd", "string"),
         ("LANGUAGE_CD", "string", "language_cd", "string"),
         ("HISPANIC", "string", "hispanic", "string"),
         ("RACE_CD", "string", "race_cd", "string"),
         ("MARITIAL_STATUS_CD", "string", "marital_status_cd", "string"),
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
        "dbTable": "patient_dimension",
        "connectionName": "postgres-glue-jdbc-connection"
    }
)

job.commit()
