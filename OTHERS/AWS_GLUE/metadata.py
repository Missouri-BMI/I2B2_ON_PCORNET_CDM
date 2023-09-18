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


tables = [
    "i2b2_cpt4",
    "vital_signs"
]

 ## check for column name case sensitivity and data types eg Date/Timestamp, int/long
for table in tables:
    source_connection_options = {
        "dbTable": table,
        "connectionName": "i2b2-metadata-prod-snowflake",
        "hashfield" : "C_FULLNAME"
    }
    source_df = glueContext.create_dynamic_frame.from_options(connection_type="custom.jdbc", connection_options= source_connection_options)
    mappings = [
       ("C_HLEVEL", "long", "c_hlevel", "int" ),
        ( "C_FULLNAME", "string", "c_fullname", "string"),
        ( "C_NAME", "string", "c_name", "string"),
        ("C_SYNONYM_CD", "string", "c_synonym_cd", "string" ),
        ( "C_VISUALATTRIBUTES", "string", "c_visualattributes", "string"),
        ("C_TOTALNUM", "long", "c_totalnum", "int"),
        ( "C_BASECODE", "string", "c_basecode", "string"),
        ("C_METADATAXML", "string", "c_metadataxml", "string" ),
        ( "C_FACTTABLECOLUMN", "string", "c_facttablecolumn", "string"),
        ("C_TABLENAME", "string", "c_tablename", "string" ),
        ("C_COLUMNNAME", "string", "c_columnname", "string" ),
        ("C_COLUMNDATATYPE", "string", "c_columndatatype", "string"),
        ( "C_OPERATOR", "string", "c_operator", "string"),
        ( "C_DIMCODE", "string", "c_dimcode", "string"),
        ("C_COMMENT", "string", "c_comment", "string"),
        ("C_TOOLTIP", "string", "c_tooltip", "string"),
        ("M_APPLIED_PATH", "string", "m_applied_path", "string" ),
        ("UPDATE_DATE", "timestamp", "update_date", "timestamp" ),
        ("DOWNLOAD_DATE", "timestamp", "download_date", "timestamp"),
        ( "IMPORT_DATE", "timestamp", "import_date", "timestamp"),
        ("SOURCESYSTEM_CD", "string", "sourcesystem_cd", "string"),
        ("VALUETYPE_CD", "string", "valuetype_cd", "string" ),
        ("M_EXCLUSION_CD", "string", "m_exclusion_cd", "string"),
        ("C_PATH", "string", "c_path", "string"),
        ("C_SYMBOL", "string", "c_symbol", "string")
    ]
    apply_mapping = ApplyMapping.apply(frame=source_df, mappings = mappings)
    statement = "truncate table "+table+";"
    dest_conn_opt = {
        "dbTable": table,
        "connectionName": "i2b2MetadataSnowflake"
    }
    
    destination_df = glueContext.write_dynamic_frame.from_options(frame=apply_mapping, connection_type="custom.jdbc", connection_options= dest_conn_opt)

# job.commit()
