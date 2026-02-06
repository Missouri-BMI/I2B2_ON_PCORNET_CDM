import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import dotenv_values

from common import *

logger = logging.getLogger(__name__)

# ---------------- defaults / knobs ----------------
BASE_ENV_DIR = os.getenv("I2B2_ENV_BASE_DIR", "/opt/airflow/env")

DEFAULT_SCHEDULE = os.getenv("I2B2_SNOWFLAKE_I2B2_SCHEDULE", None)
DEFAULT_START_DATE = pendulum.datetime(2021, 1, 1, tz="UTC")

BASE_PATH = os.getenv("I2B2_DATA_INSTALLER_BASE_PATH", "/opt/airflow/SCRIPTS/DATA_INSTALLER")
ENACT_PATH = os.getenv("I2B2_ENACT_PATH", f"{BASE_PATH}/ACT_V4_LOADER")
ENACT_DATA = os.getenv("I2B2_ENACT_DATA", f"{ENACT_PATH}/ENACT_V41_POSTGRES_I2B2_TSV")

# file format / stage names (can be overridden per .env)
DEFAULT_TSV_FORMAT = os.getenv("I2B2_TSV_FORMAT", "TSV_FORMAT")
DEFAULT_TSV_STAGE = os.getenv("I2B2_TSV_STAGE", "i2b2_ont_import_tsv")
DEFAULT_DSV_FORMAT = os.getenv("I2B2_DSV_FORMAT", "DSV_FORMAT")
DEFAULT_DSV_STAGE = os.getenv("I2B2_DSV_STAGE", "i2b2_ont_import_dsv")

PUT_PARAMETERS = os.getenv(
    "I2B2_PUT_PARAMETERS",
    "PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE",
)

# Only for sandbox using
FILTER_ACCOUNTS = set(x.strip() for x in os.getenv("I2B2_FILTER_ACCOUNTS", "deidentified").split(",") if x.strip())
FILTER_ENVS = set(x.strip() for x in os.getenv("I2B2_FILTER_ENVS", "sandbox").split(",") if x.strip())
FILTER_SITES = set(x.strip() for x in os.getenv("I2B2_FILTER_SITES", "mu").split(",") if x.strip())

REQUIRED_KEYS = [
    "CONNECTION_ID",
    "TARGET_DB", "TARGET_SCHEMA",
    "CRC_SCHEMA", "HIVE_SCHEMA", "PM_SCHEMA", "METADATA_SCHEMA", "WORKDATA_SCHEMA",
]


# ---------------- config model ----------------
@dataclass(frozen=True)
class RunConfig:
    account: str          # identified|deidentified
    environment: str      # sandbox|dev|prod
    site: str             # mu, gpc, ...
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
    Discover env files in:
      {base_env_dir}/{account}/{environment}/{site}.env

    Example:
      /opt/airflow/env/identified/dev/mu.env
      /opt/airflow/env/deidentified/prod/gpc.env
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
                    logger.error("Skipping %s (missing keys: %s)", env_path, missing)
                    continue

                configs.append(RunConfig(account, environment, site, env_path, values))

    return configs

def _add_schema_sql(schema, sql_text) -> str:
    return f"use schema {schema};\n" + sql_text

def _build_kwargs(cfg: RunConfig) -> Dict[str, str]:
    a = cfg.values

    target_db = a["TARGET_DB"]
    target_schema = f"{target_db}.{a['TARGET_SCHEMA']}"

    crc_schema = f"{target_db}.{a['CRC_SCHEMA']}"
    hive_schema = f"{target_db}.{a['HIVE_SCHEMA']}"
    pm_schema = f"{target_db}.{a['PM_SCHEMA']}"
    metadata_schema = f"{target_db}.{a['METADATA_SCHEMA']}"
    wd_schema = f"{target_db}.{a['WORKDATA_SCHEMA']}"

    # stage schema (can override)
    stage_schema_name = a.get("STAGE_SCHEMA", "enact_stage")
    stage_schema = f"{target_db}.{stage_schema_name}"

    tsv_format = a.get("TSV_FORMAT", DEFAULT_TSV_FORMAT)
    tsv_stage = a.get("TSV_STAGE", DEFAULT_TSV_STAGE)
    dsv_format = a.get("DSV_FORMAT", DEFAULT_DSV_FORMAT)
    dsv_stage = a.get("DSV_STAGE", DEFAULT_DSV_STAGE)

    enact_data_dir = a.get("ENACT_DATA_DIR", ENACT_DATA)
    local_stage = f"file://{enact_data_dir}"

    return {
        "crc_schema": crc_schema,
        "hive_schema": hive_schema,
        "metadata_schema": metadata_schema,
        "pm_schema": pm_schema,
        "wd_schema": wd_schema,
        "target_schema": target_schema,
        "stage_schema": stage_schema,
        "target_db": target_db,
        "TSV_FORMAT": tsv_format,
        "TSV_STAGE": tsv_stage,
        "DSV_FORMAT": dsv_format,
        "DSV_STAGE": dsv_stage,
        "LOCAL_STAGE": local_stage,
        "PUT_PARAMETERS": PUT_PARAMETERS,
    }


def build_dag(cfg: RunConfig) -> Optional[DAG]:
    """
    Build one DAG per config for the i2b2 enact ontology load + i2b2-data export workflow.
    """
    try:
        a = cfg.values
        snowflake_conn_id = a["CONNECTION_ID"]

        dag_id = f"i2b2_snowflake__{cfg.account}__{cfg.environment}__{cfg.site}"
        schedule = a.get("SCHEDULE", DEFAULT_SCHEDULE)

        dag = DAG(
            dag_id=dag_id,
            description=f"Export Snowflake enact ontology to i2b2-data ({cfg.account}/{cfg.environment}/{cfg.site})",
            schedule=schedule,
            start_date=DEFAULT_START_DATE,
            catchup=False,
            tags=["i2b2_snowflake", cfg.account, cfg.environment, cfg.site],
            max_active_runs=1,
            default_args={
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
            },
        )

        kwargs = _build_kwargs(cfg)

        # Resolve data/paths (can override per env)
        enact_data_dir = a.get("ENACT_DATA_DIR", ENACT_DATA)
        enact_path = a.get("ENACT_PATH", ENACT_PATH)
        base_path = a.get("BASE_PATH", BASE_PATH)

        # i2b2-data export destinations (override if your repo layout differs per env)
        crc_concept_path = a.get(
            "CRC_CONCEPT_PATH",
            f"{base_path}/i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/Crcdata/act/scripts/snowflake",
        )
        ont_path = a.get(
            "ONT_PATH",
            f"{base_path}/i2b2-data/edu.harvard.i2b2.data/Release_1-8/NewInstall/Metadata/act/scripts/snowflake",
        )

        with dag:
            # 1) ensure stage schema exists
            create_stage = SnowflakeSqlApiOperator(
                    task_id="create-i2b2-data-stage-schema",
                    snowflake_conn_id=snowflake_conn_id,
                    sql= f"CREATE OR REPLACE SCHEMA {kwargs['stage_schema']};",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0,
                )



            # 2) ENACT (pcornet) load pipeline
            with TaskGroup("enact-pcornet") as enact_pcornet:
                # create file formats / stages / upload base files
                with TaskGroup("extract") as Extract:
                    stage_act_path = f"{base_path}/CONFIGURE/COMMON/stage_act.sql"
                    sf_sql_task(
                        task_id="stage-act-in-snowflake",
                        conn_id=snowflake_conn_id,
                        sql_path=stage_act_path,
                        render_kwargs=kwargs,
                    )

                # load staged enact data in the stage schema
                with TaskGroup("load") as Load:
                    # DDL
                    with TaskGroup("ACT_DDL") as ACT_DDL:
                        enact_ddl = f"{enact_data_dir}/AA_CREATE_METADATA_TABLES_V41_POSTGRES.sql"
                        ddl_read = read_sql_from_file(enact_ddl, **kwargs)
                        final_sql = _add_schema_sql(kwargs["stage_schema"], ddl_read)

                        create_tables_task = SnowflakeSqlApiOperator(
                            task_id="ACT_DDL_METADATA",
                            snowflake_conn_id=snowflake_conn_id,
                            sql=final_sql,
                            trigger_rule=TriggerRule.ALL_SUCCESS,
                            autocommit=True,
                            retries=0,
                        )

                    # ACT_*.tsv loads
                    with TaskGroup("ACT_LOAD") as ACT_LOAD:
                        for file in sorted(os.listdir(enact_data_dir)):
                            filename = Path(file).name
                            stem = Path(file).stem
                            # Keep your original filters
                            if not filename.startswith("ACT"):
                                continue
                            if stem.endswith("PRECALC"):
                                continue

                            table_name = stem
                            if table_name.endswith("_POSTGRES"):
                                table_name = table_name.removesuffix("_POSTGRES")

                            copy_sql = f"""
                                COPY INTO {kwargs['stage_schema']}.{table_name}
                                FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/{filename}
                                FILE_FORMAT={kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};
                            """.strip()

                            SnowflakeSqlApiOperator(
                                task_id=f"load-{table_name}",
                                snowflake_conn_id=snowflake_conn_id,
                                sql=copy_sql,
                                trigger_rule=TriggerRule.ALL_SUCCESS,
                                autocommit=True,
                                retries=0,
                            )

                    # SCHEMES
                    load_schemes_sql = f"""
                        CREATE TABLE {kwargs['stage_schema']}.SCHEMES
                        (
                            C_KEY VARCHAR(50) NOT NULL,
                            C_NAME VARCHAR(50) NOT NULL,
                            C_DESCRIPTION VARCHAR(100) NULL
                        );

                        COPY INTO {kwargs['stage_schema']}.SCHEMES
                        FROM @{kwargs['stage_schema']}.{kwargs['DSV_STAGE']}/SCHEMES_V41.dsv
                        FILE_FORMAT={kwargs['stage_schema']}.{kwargs['DSV_FORMAT']};
                    """.strip()
                    load_schemes_tasks = SnowflakeSqlApiOperator(
                                task_id="load-schemes",
                                snowflake_conn_id=snowflake_conn_id,
                                sql=load_schemes_sql,
                                trigger_rule=TriggerRule.ALL_SUCCESS,
                                autocommit=True,
                                retries=0,
                            )

                    # TABLE_ACCESS
                    load_ta_sql = f"""
                        CREATE TABLE {kwargs['stage_schema']}.TABLE_ACCESS
                        (
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
                            C_ENTRY_DATE TIMESTAMP NULL,
                            C_CHANGE_DATE TIMESTAMP NULL,
                            C_STATUS_CD CHAR(1) NULL,
                            VALUETYPE_CD VARCHAR(50) NULL
                        );

                        COPY INTO {kwargs['stage_schema']}.TABLE_ACCESS
                        FROM (
                            SELECT
                                $1, $2, $3, $24, $4, $5, $6, $7, $8, $9, $10, $11,
                                $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
                            FROM @{kwargs['stage_schema']}.{kwargs['DSV_STAGE']}/TABLE_ACCESS_V41.dsv
                        )
                        FILE_FORMAT = {kwargs['stage_schema']}.{kwargs['DSV_FORMAT']};
                    """.strip()
                    load_ta_tasks = SnowflakeSqlApiOperator(
                        task_id="load-table-access",
                        snowflake_conn_id=snowflake_conn_id,
                        sql=load_ta_sql,
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        autocommit=True,
                        retries=0,
                    )

                    # CONCEPT_DIMENSION
                    load_concept_sql = f"""
                        CREATE TABLE {kwargs['stage_schema']}.CONCEPT_DIMENSION
                        (
                            CONCEPT_PATH VARCHAR(700) NOT NULL,
                            CONCEPT_CD VARCHAR(50) NULL,
                            NAME_CHAR VARCHAR(2000) NULL,
                            CONCEPT_BLOB TEXT NULL,
                            UPDATE_DATE TIMESTAMP NULL,
                            DOWNLOAD_DATE TIMESTAMP NULL,
                            IMPORT_DATE TIMESTAMP NULL,
                            SOURCESYSTEM_CD VARCHAR(50) NULL,
                            UPLOAD_ID INT NULL
                        );

                        COPY INTO {kwargs['stage_schema']}.CONCEPT_DIMENSION
                        FROM @{kwargs['stage_schema']}.{kwargs['TSV_STAGE']}/CONCEPT_DIMENSION_V41.tsv
                        FILE_FORMAT={kwargs['stage_schema']}.{kwargs['TSV_FORMAT']};
                    """.strip()

                    load_concept_tasks = SnowflakeSqlApiOperator(
                        task_id="load-concepts",
                        snowflake_conn_id=snowflake_conn_id,
                        sql=load_concept_sql,
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        autocommit=True,
                        retries=0,
                    )

                    ACT_DDL >> ACT_LOAD >> [load_schemes_tasks, load_ta_tasks, load_concept_tasks]

                # transform
                with TaskGroup("transform") as Transform:
                    harmonize_path = a.get("HARMONIZE_PATH", f"{enact_path}/scripts/harmonize-proc.sql")
                    harmonize_proc = sf_sql_task(
                        task_id="act_harmonize_proc",
                        conn_id=snowflake_conn_id,
                        sql_path=harmonize_path,
                        render_kwargs=kwargs,
                    )

                    harmonize_task = SnowflakeSqlApiOperator(
                        task_id="act_harmonize",
                        snowflake_conn_id=snowflake_conn_id,
                        sql=f"USE SCHEMA {kwargs['stage_schema']}; call harmonize_proc();",
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        autocommit=True,
                        retries=0,
                    )
                
                    harmonize_proc >> harmonize_task

                Extract >> Load >> Transform

            # 3) i2b2-data export
            with TaskGroup("i2b2-data-export") as i2b2_data_export_task:
                with TaskGroup("stage-table") as stage_table_task:
                    sql_file = f"{base_path}/CONFIGURE/COMMON/extract_concept.sql"
                    create_tables_task = sf_sql_task(
                        task_id="concept-dimension-export",
                        conn_id=snowflake_conn_id,
                        sql_path=sql_file,
                        render_kwargs=kwargs,
                    )


                with TaskGroup("export-concepts") as export_concepts_task:
                    concept_export_path = f"file://{crc_concept_path}"
                    download_concept = SnowflakeSqlApiOperator(
                        task_id="download-concept-dimension",
                        snowflake_conn_id=snowflake_conn_id,
                        sql=f"GET @{kwargs['stage_schema']}.CSV_STAGE/CONCEPT_DIMENSION.csv {concept_export_path} OVERWRITE=TRUE",
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        autocommit=True,
                        retries=0,
                    )
                    
                    zip_concept = BashOperator(
                        task_id="zip_concept_files",
                        bash_command=f"""
                        i=1;
                        for file in {crc_concept_path}/*.csv.gz; do
                            zip -j {crc_concept_path}/crcdata${{i}}.zip "$file";
                            rm "$file";
                            i=$((i+1));
                        done
                        """,
                        retries=0,
                    )
                    download_concept >> zip_concept

                with TaskGroup("export-metadata") as export_metadata_task:
                    ont_export_path = f"file://{ont_path}"
                    download_ont = SnowflakeSqlApiOperator(
                        task_id="download-act-metadata",
                        snowflake_conn_id=snowflake_conn_id,
                        sql=f"GET @{kwargs['stage_schema']}.CSV_STAGE {ont_export_path} PATTERN='.*\\.csv.gz' OVERWRITE=TRUE",
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        autocommit=True,
                        retries=0,
                    )
                
                    zip_meta = BashOperator(
                        task_id="zip_metadata_files",
                        bash_command=f"""
                        i=1;
                        for file in {ont_path}/*.csv.gz; do
                            if [[ "$(basename "$file")" == CONCEPT_DIMENSION* ]]; then
                                rm "$file";
                            else
                                zip -j {ont_path}/metadata${{i}}.zip "$file";
                                rm "$file";
                                i=$((i+1));
                            fi
                        done
                        """,
                        retries=0,
                    )
                    download_ont >> zip_meta

                stage_table_task >> export_concepts_task >> export_metadata_task

            # order
            create_stage >> enact_pcornet >> i2b2_data_export_task

        return dag

    except Exception:
        logger.exception("Failed to build snowflake_i2b2 DAG for config: %s", cfg.env_path)
        return None


# ---------------- register DAGs ----------------
_configs = discover_configs(BASE_ENV_DIR)
if not _configs:
    logger.warning("No env configs found under %s", BASE_ENV_DIR)

for _cfg in _configs:
    _dag = build_dag(_cfg)
    if _dag:
        globals()[_dag.dag_id] = _dag
