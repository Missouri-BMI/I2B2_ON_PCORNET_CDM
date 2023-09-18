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
            "dbTable": "DEID_PROVIDER_DIMENSION",
            "connectionName": "snowflake-july22",
            'hashfield': 'PROVIDER_ID'
        }
    )
)

# Script generated for node Apply Mapping
target_data = ApplyMapping.apply(
    frame=source_data,
    mappings=[
         ("PROVIDER_ID", "string", "provider_id", "string"),
         ("PROVIDER_SEX", "string", "provider_sex", "string"),
         ("PROVIDER_SPECIALTY_PRIMARY", "string", "provider_specialty_primary", "string"),
         ("PROVIDER_NPI", "string", "provider_npi", "string"),
         ("PROVIDER_NPI_FLAG", "string", "provider_npi_flag", "string"),
         ("RAW_PROVIDER_SPECIALTY_PRIMARY", "string", "raw_provider_specialty_primary", "string"),
         ("UPDATE_DATE", "timestamp", "update_date", "date"),
         ("DOWNLOAD_DATE", "timestamp", "download_date", "date"),
         ("IMPORT_DATE", "timestamp", "import_date", "date"),
         ("SOURCESYSTEM_CD", "string", "sourcesystem_cd", "string"),
         ("UPLOAD_ID", "long", "upload_id", "int")
        
    ]
)

# Script generated for node postgres-jdbc-connector
glueContext.write_dynamic_frame.from_options(
    frame=target_data,
    connection_type="custom.jdbc",
    connection_options={
        "dbTable": "provider_dimension",
        "connectionName": "postgres-glue-jdbc-connection"
    }
)

job.commit()
