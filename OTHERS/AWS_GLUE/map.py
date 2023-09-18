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


source_connection_options = {
    "dbTable": "table_access",
    "connectionName": "i2b2-metadata",
    "hashfield" : "c_fullname"
}

source_df = glueContext.create_dynamic_frame.from_options(connection_type="custom.jdbc", connection_options= source_connection_options)
source_df.printSchema()
mappings = [
    
    ("c_table_cd", "string", "C_TABLE_CD", "string"),
    ("c_table_name", "string", "C_TABLE_NAME", "string"),
    ("c_protected_access", "string", "C_PROTECTED_ACCESS", "string"),
    ("c_ontology_protection", "string", "C_ONTOLOGY_PROTECTION", "string"),
    ("c_hlevel", "int", "C_HLEVEL", "long"),
    ("c_fullname", "string", "C_FULLNAME", "string"),
    ("c_name", "string", "C_NAME", "string"),
    ("c_synonym_cd", "string", "C_SYNONYM_CD", "string"),
    ("c_visualattributes", "string", "C_VISUALATTRIBUTES", "string"),
    ("c_totalnum", "int", "C_TOTALNUM", "long"),
    ("c_basecode", "string", "C_BASECODE", "string"),
    ("c_metadataxml", "string", "C_METADATAXML", "string"),
    ("c_facttablecolumn", "string", "C_FACTTABLECOLUMN", "string"),
    ("c_dimtablename", "string", "C_DIMTABLENAME", "string"),
    ("c_columnname", "string", "C_COLUMNNAME", "string"),
    ("c_columndatatype", "string", "C_COLUMNDATATYPE", "string"),
    ("c_operator", "string", "C_OPERATOR", "string"),
    ("c_dimcode", "string", "C_DIMCODE", "string"),
    ("c_comment", "string", "C_COMMENT", "string"),
    ("c_tooltip", "string", "C_TOOLTIP", "string"),
    ("c_entry_date", "timestamp", "C_ENTRY_DATE", "timestamp"),
    ("c_change_date", "timestamp", "C_CHANGE_DATE", "timestamp"),
    ("c_status_cd", "string", "C_STATUS_CD", "string"),
    ("valuetype_cd", "string", "VALUETYPE_CD", "string")
]
apply_mapping = ApplyMapping.apply(frame=source_df, mappings = mappings)
dest_conn_opt = {
    "dbTable": "TABLE_ACCESS",
    "connectionName": "snowflake-ontology-act-prod"
}
    
destination_df = glueContext.write_dynamic_frame.from_options(frame=apply_mapping, connection_type="custom.jdbc", connection_options= dest_conn_opt)

# job.commit()
