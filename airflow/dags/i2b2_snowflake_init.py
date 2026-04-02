from fileinput import filename
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pendulum
from airflow.models import DAG
from airflow.sdk import TaskGroup
from airflow.sdk import TriggerRule
from dotenv import dotenv_values

from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common import read_sql_from_file, sf_sql_task

logger = logging.getLogger(__name__)

# ────────────────────────── defaults / knobs ──────────────────────────
BASE_ENV_DIR = os.getenv("I2B2_ENV_BASE_DIR", "/opt/airflow/env")

DEFAULT_SCHEDULE = os.getenv("I2B2_SNOWFLAKE_I2B2_SCHEDULE", None)
DEFAULT_START_DATE = pendulum.datetime(2021, 1, 1, tz="UTC")

BASE_PATH = os.getenv(
    "I2B2_DATA_INSTALLER_BASE_PATH", "/opt/airflow/SCRIPTS/DATA_INSTALLER"
)
ENACT_PATH = os.getenv("I2B2_ENACT_PATH", f"{BASE_PATH}/ACT_V42_LOADER")
ENACT_DATA = os.getenv("I2B2_ENACT_DATA", f"{ENACT_PATH}/tsv")

# File format / stage names (can be overridden per .env)
DEFAULT_TSV_FORMAT = os.getenv("I2B2_TSV_FORMAT", "TSV_FORMAT")
DEFAULT_TSV_STAGE = os.getenv("I2B2_TSV_STAGE", "i2b2_ont_import_tsv")
DEFAULT_DSV_FORMAT = os.getenv("I2B2_DSV_FORMAT", "DSV_FORMAT")
DEFAULT_DSV_STAGE = os.getenv("I2B2_DSV_STAGE", "i2b2_ont_import_dsv")

PUT_PARAMETERS = os.getenv(
    "I2B2_PUT_PARAMETERS",
    "PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE",
)

# Filter to specific account/env/site combos (comma-separated)
FILTER_ACCOUNTS = set(
    x.strip()
    for x in os.getenv("I2B2_FILTER_ACCOUNTS", "deidentified").split(",")
    if x.strip()
)
FILTER_ENVS = set(
    x.strip()
    for x in os.getenv("I2B2_FILTER_ENVS", "sandbox").split(",")
    if x.strip()
)
FILTER_SITES = set(
    x.strip()
    for x in os.getenv("I2B2_FILTER_SITES", "mu").split(",")
    if x.strip()
)

REQUIRED_KEYS = [
    "CONNECTION_ID",
    "TARGET_DB",
    "TARGET_SCHEMA",
    "CRC_SCHEMA",
    "HIVE_SCHEMA",
    "PM_SCHEMA",
    "METADATA_SCHEMA",
    "WORKDATA_SCHEMA",
]


# ────────────────────────── config model ──────────────────────────
@dataclass(frozen=True)
class RunConfig:
    account: str  # identified | deidentified
    environment: str  # sandbox | dev | prod
    site: str  # mu, gpc, washu, pcornet …
    env_path: str
    values: Dict[str, str]


def _should_include(account: str, environment: str, site: str) -> bool:
    if FILTER_ACCOUNTS and account not in FILTER_ACCOUNTS:
        return False
    if FILTER_ENVS and environment not in FILTER_ENVS:
        return False
    if FILTER_SITES and site not in FILTER_SITES:
        return False
    return True


def discover_configs(base_env_dir: str) -> List[RunConfig]:
    """
    Walk {base_env_dir}/{account}/{environment}/{site}.env and return
    one RunConfig per valid .env file that passes the filter.
    """
    configs: List[RunConfig] = []

    for account in ("identified", "deidentified"):
        account_dir = os.path.join(base_env_dir, account)
        if not os.path.isdir(account_dir):
            continue

        for environment in ("sandbox", "dev", "prod"):
            env_dir = os.path.join(account_dir, environment)
            if not os.path.isdir(env_dir):
                continue

            for fname in sorted(os.listdir(env_dir)):
                if not fname.endswith(".env"):
                    continue

                site = fname[:-4]
                if not _should_include(account, environment, site):
                    continue

                env_path = os.path.join(env_dir, fname)
                raw = dotenv_values(env_path)
                values = {k: str(v) for k, v in raw.items() if v is not None}

                missing = [k for k in REQUIRED_KEYS if not values.get(k)]
                if missing:
                    logger.error(
                        "Skipping %s (missing keys: %s)", env_path, missing
                    )
                    continue

                configs.append(
                    RunConfig(account, environment, site, env_path, values)
                )

    return configs


def _add_schema_sql(schema: str, sql_text: str) -> str:
    return f"USE SCHEMA {schema};\n{sql_text}"


def _build_kwargs(cfg: RunConfig) -> Dict[str, str]:
    """
    Build the full Jinja / f-string rendering context from a RunConfig.
    """
    a = cfg.values

    target_db = a["TARGET_DB"]
    target_schema = f"{target_db}.{a['TARGET_SCHEMA']}"

    crc_schema = f"{target_db}.{a['CRC_SCHEMA']}"
    hive_schema = f"{target_db}.{a['HIVE_SCHEMA']}"
    pm_schema = f"{target_db}.{a['PM_SCHEMA']}"
    metadata_schema = f"{target_db}.{a['METADATA_SCHEMA']}"
    wd_schema = f"{target_db}.{a['WORKDATA_SCHEMA']}"

    stage_schema_name = a.get("STAGE_SCHEMA", "enact_stage")
    stage_schema = f"{target_db}.{stage_schema_name}"

    # Source schema used by site-specific Jinja templates (HARVEST queries, etc.)
    source_schema = a.get("SOURCE_SCHEMA", target_schema)

    tsv_format = a.get("TSV_FORMAT", DEFAULT_TSV_FORMAT)
    tsv_stage = a.get("TSV_STAGE", DEFAULT_TSV_STAGE)
    dsv_format = a.get("DSV_FORMAT", DEFAULT_DSV_FORMAT)
    dsv_stage = a.get("DSV_STAGE", DEFAULT_DSV_STAGE)

    enact_data_dir = a.get("ENACT_DATA_DIR", ENACT_DATA)
    local_stage = f"file://{enact_data_dir}"

    return {
        # site identifier — used by {% if site == 'mu' %} etc.
        "site": cfg.site,
        # schemas
        "crc_schema": crc_schema,
        "hive_schema": hive_schema,
        "metadata_schema": metadata_schema,
        "pm_schema": pm_schema,
        "wd_schema": wd_schema,
        "target_schema": target_schema,
        "stage_schema": stage_schema,
        "source_schema": source_schema,
        "target_db": target_db,
        # file format / stage names
        "TSV_FORMAT": tsv_format,
        "TSV_STAGE": tsv_stage,
        "DSV_FORMAT": dsv_format,
        "DSV_STAGE": dsv_stage,
        # file upload
        "LOCAL_STAGE": local_stage,
        "PUT_PARAMETERS": PUT_PARAMETERS,
    }

# ────────────────────────── DAG builder ──────────────────────────
def build_dag(cfg: RunConfig) -> Optional[DAG]:
    """
    Build an Airflow DAG that:
      1. Provisions a Snowflake scratch schema with internal stages
      2. Uploads ACT/ENACT ontology TSV files and loads them into tables
      3. Harmonises the ontology data via a stored procedure
      4. Exports the result as i2b2-data artifacts (CSV → ZIP)
    """
    try:
        a = cfg.values
        snowflake_conn_id = a["CONNECTION_ID"]

        dag_id = f"i2b2_enact_ontology__{cfg.account}__{cfg.environment}__{cfg.site}"
        schedule = a.get("SCHEDULE", DEFAULT_SCHEDULE)

        dag = DAG(
            dag_id=dag_id,
            description=(
                f"Load ACT/ENACT ontology into Snowflake and export "
                f"i2b2-data artifacts ({cfg.account}/{cfg.environment}/{cfg.site})"
            ),
            schedule=schedule,
            start_date=DEFAULT_START_DATE,
            catchup=False,
            tags=[
                "i2b2",
                "enact-ontology",
                cfg.account,
                cfg.environment,
                cfg.site,
            ],
            max_active_runs=1,
            default_args={
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
            },
        )

        kwargs = _build_kwargs(cfg)

        # Resolve paths (overridable per .env)
        enact_data_dir = a.get("ENACT_DATA_DIR", ENACT_DATA)
        enact_path = a.get("ENACT_PATH", ENACT_PATH)
        base_path = a.get("BASE_PATH", BASE_PATH)

        crc_concept_path = a.get(
            "CRC_CONCEPT_PATH",
            f"{base_path}/i2b2-data/edu.harvard.i2b2.data/"
            f"Release_1-8/NewInstall/Crcdata/act/scripts/snowflake",
        )
        ont_path = a.get(
            "ONT_PATH",
            f"{base_path}/i2b2-data/edu.harvard.i2b2.data/"
            f"Release_1-8/NewInstall/Metadata/act/scripts/snowflake",
        )

        with dag:

            # ── 1. Provision scratch schema ──────────────────────────
            init_stage_schema = SnowflakeSqlApiOperator(
                task_id="init_stage_schema",
                snowflake_conn_id=snowflake_conn_id,
                sql=f"CREATE OR REPLACE SCHEMA {kwargs['stage_schema']};",
                autocommit=True,
            )
            # ── 2a. Create file formats + internal stages (SQL API) ──
            stage_ddl_sql = f"""
                CREATE OR REPLACE FILE FORMAT {kwargs['stage_schema']}.{kwargs['TSV_FORMAT']}
                    TYPE = CSV
                    FIELD_DELIMITER = '\\t'
                    ESCAPE = NONE
                    NULL_IF = ('NULL')
                    COMPRESSION = AUTO
                    DATE_FORMAT = 'yyyy-MM-dd'
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                    SKIP_HEADER = 1;

                CREATE OR REPLACE FILE FORMAT {kwargs['stage_schema']}.{kwargs['TSV_FORMAT']}_2
                    TYPE = CSV
                    FIELD_DELIMITER = '\\t'
                    ESCAPE = NONE
                    NULL_IF = ('NULL')
                    COMPRESSION = AUTO
                    DATE_FORMAT = 'yyyy/MM/dd'
                    TIMESTAMP_FORMAT = 'YYYY/MM/DD'
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                    SKIP_HEADER = 1;

                CREATE OR REPLACE STAGE {kwargs['stage_schema']}.{kwargs['TSV_STAGE']}
                    FILE_FORMAT = {kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};

            """.strip()

            create_formats_and_stages = SnowflakeSqlApiOperator(
                task_id="create_file_formats_and_stages",
                snowflake_conn_id=snowflake_conn_id,
                sql=stage_ddl_sql,
                autocommit=True,
            )

            # ── 2b. Upload files (PUT needs Python connector) ────────
            upload_tsv = SQLExecuteQueryOperator(
                task_id="put_tsv_files",
                conn_id=snowflake_conn_id,
                sql=(
                    f"PUT {kwargs['LOCAL_STAGE']}/*.tsv "
                    f"@{kwargs['stage_schema']}.{kwargs['TSV_STAGE']} "
                    f"{kwargs['PUT_PARAMETERS']};"
                ),
                autocommit=True,
            )

            # ── 2c. DDL + COPY INTO ─────────────────────────────────
            with TaskGroup("load_staged_data") as load_staged_data:

                # DDL — create ACT metadata tables
                enact_ddl = f"{enact_data_dir}/AA_CREATE_METADATA_TABLES_V42_SNOWFLAKE.sql"
                ddl_read = read_sql_from_file(enact_ddl, **kwargs)
                final_sql = _add_schema_sql(kwargs["stage_schema"], ddl_read)

                execute_act_ddl = SnowflakeSqlApiOperator(
                    task_id="execute_act_ddl",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=final_sql,
                    autocommit=True,
                )
                
                with TaskGroup("load_act_data") as load_act_data:
                    tasks = []
                    start = EmptyOperator(task_id="start_load_act_data")
                    end = EmptyOperator(task_id="all_load_act_data_sql_finished")
                    
                    FILE_TABLE_MAP = {
                        "ACT_COVID_V41.tsv":                "ACT_COVID_V41",
                        "ACT_CPT4_PX_V42.tsv":              "ACT_CPT4_PX_V42",
                        "ACT_DEM_POSTGRES_V42.tsv":         "ACT_DEM_V42",
                        "ACT_HCPCS_PX_V42.tsv":             "ACT_HCPCS_PX_V42",
                        "ACT_ICD9CM_DX_V4.tsv":             "ACT_ICD9CM_DX_V4",
                        "ACT_ICD9CM_PX_V4.tsv":             "ACT_ICD9CM_PX_V4",
                        "ACT_ICD10CM_DX_V42.tsv":           "ACT_ICD10CM_DX_V42",
                        "ACT_ICD10PCS_PX_V42.tsv":          "ACT_ICD10PCS_PX_V42",
                        "ACT_ICD10_ICD9_DX_V4.tsv":         "ACT_ICD10_ICD9_DX_V4",
                        "ACT_LOINC_LAB_PROV_V42.tsv":       "ACT_LOINC_LAB_PROV_V42",
                        "ACT_LOINC_LAB_V42.tsv":            "ACT_LOINC_LAB_V42",
                        "ACT_MED_ALPHA_V42.tsv":            "ACT_MED_ALPHA_V42",
                        "ACT_MED_VA_V42.tsv":               "ACT_MED_VA_V42",
                        "ACT_RESEARCH_V42A_POSTGRES.tsv":        "ACT_RESEARCH_V42",
                        "ACT_SDOH_V42.tsv":                     "ACT_SDOH_V42",
                        "ACT_VAX_V42.tsv":                      "ACT_VAX_V42",
                        "ACT_VISIT_DETAILS_V41_POSTGRES.tsv": "ACT_VISIT_DETAILS_V41",
                        "ACT_VITAL_SIGNS_V4.tsv":           "ACT_VITAL_SIGNS_V4",
                        "ACT_ZIPCODE_V41.tsv":              "ACT_ZIPCODE_V41",
                    }

                    for filename, table_name in FILE_TABLE_MAP.items():
                        if table_name in ("ACT_RESEARCH_V42", "ACT_VISIT_DETAILS_V41"): #yyyy/MM/dd date format for CD_ files
                            copy_sql = (
                            f"COPY INTO {kwargs['stage_schema']}.{table_name} "
                            f"FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/{filename} "
                            f"FILE_FORMAT={kwargs['stage_schema']}.{kwargs['TSV_FORMAT']}_2;"
                        )
                        else:
                            copy_sql = (
                            f"COPY INTO {kwargs['stage_schema']}.{table_name} " #yyyy-MM-dd date format for CD_ files
                            f"FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/{filename} "
                            f"FILE_FORMAT={kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};"
                        )
                            
                        t = SnowflakeSqlApiOperator(
                            task_id=f"copy_{table_name.lower()}",
                            snowflake_conn_id=snowflake_conn_id,
                            sql=copy_sql,
                            autocommit=True,
                        )
                        tasks.append(t)
                    start >> tasks >> end

                # SCHEMES
                load_schemes = SnowflakeSqlApiOperator(
                    task_id="load_schemes",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=f"""
                        CREATE TABLE IF NOT EXISTS {kwargs['stage_schema']}.SCHEMES (
                            C_KEY VARCHAR(50) NOT NULL,
                            C_NAME VARCHAR(50) NOT NULL,
                            C_DESCRIPTION VARCHAR(100) NULL
                        );
                        COPY INTO {kwargs['stage_schema']}.SCHEMES
                        FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/SCHEMES_V42.tsv
                        FILE_FORMAT={kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};
                    """.strip(),
                    autocommit=True,
                )

                # TABLE_ACCESS (column reorder via SELECT transform)
                load_table_access = SnowflakeSqlApiOperator(
                    task_id="load_table_access",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=f"""
                        CREATE TABLE IF NOT EXISTS {kwargs['stage_schema']}.TABLE_ACCESS (
                            C_TABLE_CD VARCHAR(50) NOT NULL,
                            C_TABLE_NAME VARCHAR(50) NOT NULL,
                            C_PROTECTED_ACCESS CHAR(1) NULL,
                            C_ONTOLOGY_PROTECTION TEXT NULL,
                            C_HLEVEL INT NOT NULL,
                            C_FULLNAME VARCHAR(700) NOT NULL,
                            C_NAME VARCHAR(2000) NOT NULL,
                            C_SYNONYM_CD CHAR(1) NOT NULL,
                            C_VISUALATTRIBUTES CHAR(3) NOT NULL,
                            C_TOTALNUM INT NULL,
                            C_BASECODE VARCHAR(50) NULL,
                            C_METADATAXML TEXT NULL,
                            C_FACTTABLECOLUMN VARCHAR(50) NOT NULL,
                            C_DIMTABLENAME VARCHAR(50) NOT NULL,
                            C_COLUMNNAME VARCHAR(50) NOT NULL,
                            C_COLUMNDATATYPE VARCHAR(50) NOT NULL,
                            C_OPERATOR VARCHAR(10) NOT NULL,
                            C_DIMCODE VARCHAR(700) NOT NULL,
                            C_COMMENT TEXT NULL,
                            C_TOOLTIP VARCHAR(900) NULL,
                            C_ENTRY_DATE TIMESTAMP_NTZ NULL,
                            C_CHANGE_DATE TIMESTAMP_NTZ NULL,
                            C_STATUS_CD CHAR(1) NULL,
                            VALUETYPE_CD VARCHAR(50) NULL
                        );
                        COPY INTO {kwargs['stage_schema']}.TABLE_ACCESS
                        FROM (
                            SELECT
                                $1, $2, $3, $22, $4, $5, $6, $7, $8, $9, $10, NULL, $11,
                                $12, $13, $14, $15, $16, NULL, $17, $18, $19, $20, $21
                            FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/TABLE_ACCESS.tsv
                        )
                        FILE_FORMAT = {kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};
                    """.strip(),
                    autocommit=True,
                )

                # CONCEPT_DIMENSION (pattern-match all CD_*.tsv files)
                load_concept_dimension = SnowflakeSqlApiOperator(
                    task_id="load_concept_dimension",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=f"""
                        CREATE TABLE IF NOT EXISTS {kwargs['stage_schema']}.CONCEPT_DIMENSION (
                            CONCEPT_PATH VARCHAR(700) NOT NULL,
                            CONCEPT_CD VARCHAR(50) NULL,
                            NAME_CHAR VARCHAR(2000) NULL,
                            CONCEPT_BLOB TEXT NULL,
                            UPDATE_DATE TIMESTAMP_NTZ NULL,
                            DOWNLOAD_DATE TIMESTAMP_NTZ NULL,
                            IMPORT_DATE TIMESTAMP_NTZ NULL,
                            SOURCESYSTEM_CD VARCHAR(50) NULL,
                            UPLOAD_ID INT NULL
                        );

                        COPY INTO {kwargs['stage_schema']}.CONCEPT_DIMENSION
                        FROM (
                            SELECT $1, $2, $3, NULL, $4, $5, $6, $7, $8
                            FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}    
                        )
                        PATTERN='.*\\/CD_.*[.]tsv[.]gz'
                        FILE_FORMAT = {kwargs['stage_schema']}.{kwargs['TSV_FORMAT']}_2;--yyyy/MM/dd date format for CD_ files
                    """.strip(),
                    autocommit=True,
                )

                # Internal wiring: DDL → COPY tasks → reference loads
                execute_act_ddl >> load_act_data >> [
                    load_schemes,
                    load_table_access,
                    load_concept_dimension,
                ]
                
           

            # ── 2d. Harmonize ────────────────────────────────────────
            harmonize_path = a.get(
                "HARMONIZE_PATH",
                f"{enact_path}/scripts/harmonize-proc.sql",
            )
            create_harmonize_proc = sf_sql_task(
                task_id="create_harmonize_procedure",
                conn_id=snowflake_conn_id,
                sql_path=harmonize_path,
                render_kwargs=kwargs,
            )

            run_harmonize_proc = SnowflakeSqlApiOperator(
                task_id="run_harmonize_procedure",
                snowflake_conn_id=snowflake_conn_id,
                sql=(
                    f"USE SCHEMA {kwargs['stage_schema']}; "
                    f"CALL harmonize_proc();"
                ),
                autocommit=True,
            )

            # ── 3a. Prepare export staging tables ────────────────────
            sql_file = f"{base_path}/CONFIGURE/COMMON/extract_concept.sql"
            create_concept_export = sf_sql_task(
                task_id="create_concept_export_table",
                conn_id=snowflake_conn_id,
                sql_path=sql_file,
                render_kwargs=kwargs,
            )

            # ── 3b. Download + zip concept dimension ──
            concept_export_path = f"file://{crc_concept_path}"
            download_concepts = SQLExecuteQueryOperator(
                task_id="get_concept_parquet_from_stage",
                conn_id=snowflake_conn_id,
                sql=(
                    f"GET @{kwargs['stage_schema']}.PARQUET_STAGE_CONCEPT/"
                    f" {concept_export_path} OVERWRITE=TRUE"
                ),
                autocommit=True,
            )
            zip_concepts = BashOperator(
            task_id="zip_concept_parquet_files",
            bash_command=(
                f'cd {crc_concept_path} && {{ '
                f'i=1; size=0; MAX=52428800; '
                f'table="CONCEPT_DIMENSION"; '
                f'for file in CONCEPT_DIMENSION*.snappy.parquet; do '
                f'  fsize=$(stat -c%s "$file"); '
                f'  if [ $size -gt 0 ] && [ $((size + fsize)) -gt $MAX ]; then '
                f'    i=$((i+1)); size=0; '
                f'  fi; '
                f'  zip -j crcdata_${{table}}_${{i}}.zip "$file"; '
                f'  rm "$file"; '
                f'  size=$((size + fsize)); '
                f'done; }}'
                ),
            )
            # ── 3c. Download + zip ontology metadata ──
            ont_export_path = f"file://{ont_path}"
            download_metadata = SQLExecuteQueryOperator(
                task_id="get_metadata_parquet_from_stage",
                conn_id=snowflake_conn_id,
                sql=(
                    f"GET @{kwargs['stage_schema']}.PARQUET_STAGE_ONT/"
                    f" {ont_export_path} OVERWRITE=TRUE"
                ),
                autocommit=True,
            )
            zip_metadata = BashOperator(
                task_id="zip_metadata_parquet_files",
                bash_command=(
                    f'cd {ont_path} && {{ '
                    f'i=1; size=0; MAX=52428800; '
                    f'prev_table=""; '
                    f'for file in *.snappy.parquet; do '
                    f'  table=$(echo "$file" | sed "s/\\.parquet_.*//"); '
                    f'  fsize=$(stat -c%s "$file"); '
                    f'  if [ "$table" != "$prev_table" ] && [ -n "$prev_table" ]; then '
                    f'    i=1; size=0; '
                    f'  elif [ $size -gt 0 ] && [ $((size + fsize)) -gt $MAX ]; then '
                    f'    i=$((i+1)); size=0; '
                    f'  fi; '
                    f'  zip -j metadata_${{table}}_${{i}}.zip "$file"; '
                    f'  rm "$file"; '
                    f'  size=$((size + fsize)); '
                    f'  prev_table="$table"; '
                    f'done; }}'
                ),
            )
            init_stage_schema >> create_formats_and_stages >> upload_tsv >> load_staged_data >> create_harmonize_proc >> run_harmonize_proc >> create_concept_export >> download_concepts >> download_metadata >> zip_concepts >> zip_metadata
        # init_stage_schema >> create_formats_and_stages >> [upload_tsv, upload_dsv] >> load_staged_data >> create_harmonize_proc >> run_harmonize_proc >> create_concept_export >> [download_concepts, download_metadata] >> [zip_concepts, zip_metadata]

        return dag

    except Exception:
        logger.exception(
            "Failed to build i2b2 enact ontology DAG for config: %s",
            cfg.env_path,
        )
        return None


# ────────────────────────── register DAGs ──────────────────────────
_configs = discover_configs(BASE_ENV_DIR)
if not _configs:
    logger.warning("No env configs found under %s", BASE_ENV_DIR)

for _cfg in _configs:
    _dag = build_dag(_cfg)
    if _dag:
        globals()[_dag.dag_id] = _dag