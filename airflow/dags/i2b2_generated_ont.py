import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import pendulum
from airflow.models.dag import DAG
from airflow.sdk import TaskGroup
from airflow.sdk import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from dotenv import dotenv_values

from common import *

logger = logging.getLogger(__name__)

# --------- constants / defaults ---------
BASE_ENV_DIR = os.getenv("I2B2_ENV_BASE_DIR", "/opt/airflow/env")
DEFAULT_SCHEDULE = os.getenv("I2B2_GENERATED_ONT_SCHEDULE", None)
DEFAULT_START_DATE = pendulum.datetime(2021, 1, 1, tz="UTC")

BASE_PATH = os.getenv("I2B2_GENERATED_ONT_BASE_PATH", "/opt/airflow/SCRIPTS/DATA_INSTALLER/GENERATED_ONT")

# Available in mu, gpc dev, prod, sandbox only
FILTER_ACCOUNTS = set(x.strip() for x in os.getenv("I2B2_FILTER_ACCOUNTS", "deidentified").split(",") if x.strip())
FILTER_ENVS = set(x.strip() for x in os.getenv("I2B2_FILTER_ENVS", "dev, prod, sandbox").split(",") if x.strip())
FILTER_SITES = set(x.strip() for x in os.getenv("I2B2_FILTER_SITES", "mu, gpc").split(",") if x.strip())

REQUIRED_KEYS = [
    "CONNECTION_ID",
    "PROJECT_DB",
    "SOURCE_DB", "SOURCE_SCHEMA",
    "TARGET_DB", "TARGET_SCHEMA",
    "CRC_SCHEMA", "HIVE_SCHEMA", "PM_SCHEMA", "METADATA_SCHEMA", "WORKDATA_SCHEMA",
]


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


def _build_kwargs(cfg: RunConfig) -> Dict[str, str]:
    a = cfg.values
    effective_site = resolve_site(cfg.account, cfg.site)

    source_schema = f"{a['SOURCE_DB']}.{a['SOURCE_SCHEMA']}"
    target_db = a["TARGET_DB"]
    target_schema = f"{target_db}.{a['TARGET_SCHEMA']}"
    project_db = a["PROJECT_DB"]

    crc_schema = f"{target_db}.{a['CRC_SCHEMA']}"
    hive_schema = f"{target_db}.{a['HIVE_SCHEMA']}"
    pm_schema = f"{target_db}.{a['PM_SCHEMA']}"
    metadata_schema = f"{target_db}.{a['METADATA_SCHEMA']}"
    wd_schema = f"{target_db}.{a['WORKDATA_SCHEMA']}"

    project_pm = f"{project_db}.{a['PM_SCHEMA']}"
    project_hive = f"{project_db}.{a['HIVE_SCHEMA']}"

    return {
        "metadata_schema": metadata_schema,
        "crc_schema": crc_schema,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "pm_schema": pm_schema,
        "hive_schema": hive_schema,
        "wd_schema": wd_schema,
        "project_pm": project_pm,
        "project_hive": project_hive,
        "target_db": target_db,
        "site": effective_site
    }


def build_dag(cfg: RunConfig) -> Optional[DAG]:
    """
    Build one DAG per config for generated/custom ontology build.
    """
    try:
        a = cfg.values
        snowflake_conn_id = a["CONNECTION_ID"]

        dag_id = f"i2b2_generated_ontology__{cfg.account}__{cfg.environment}__{cfg.site}"
        schedule = a.get("SCHEDULE", DEFAULT_SCHEDULE)

        dag = DAG(
            dag_id=dag_id,
            description=f"generate custom ontology in i2b2 ({cfg.account}/{cfg.environment}/{cfg.site}",
            schedule=schedule,
            start_date=DEFAULT_START_DATE,
            catchup=False,
            max_active_runs=1,
            tags=["i2b2_custom_ontology", cfg.account, cfg.environment, cfg.site],
            default_args={
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
            },
        )

        kwargs = _build_kwargs(cfg)

        # --- paths (match your original layout) ---
        acs_script = f"{BASE_PATH}/ACS/build.sql"
        adi_script = f"{BASE_PATH}/ADI_RANKING/build.sql"
        medlist_script = f"{BASE_PATH}/MED_LIST/build.sql"

        facility_ontology_path = f"{BASE_PATH}/FACILITY_ONTOLOGY"
        facility_id_path = f"{facility_ontology_path}/facility_id.sql"
        facility_location_path = f"{facility_ontology_path}/facility_location.sql"

        # SITES folder contains per-project TSVs: {BASE_PATH}/SITES/{project}/*.tsv
        sites_root = f"{BASE_PATH}/SITES"

        # Optional: keep these declared for future expansion, but not used here:
        # loinc_doc_path = f"{BASE_PATH}/LOINC_DOC_ONTOLOGY"
        # naaccr_path = f"{BASE_PATH}/NAACCR_SNOWFLAKE"

        with dag:
        
            facility_id = sf_sql_task(
                task_id="build_facility_id",
                conn_id=snowflake_conn_id,
                sql_path=facility_id_path,
                render_kwargs=kwargs,
            )

            facility_location = sf_sql_task(
                task_id="build_facility_location",
                conn_id=snowflake_conn_id,
                sql_path=facility_location_path,
                render_kwargs=kwargs,
            )
                

            # 2) MEDLIST / ADI / ACS builds
            medlist = sf_sql_task(
                task_id="build_medlist",
                conn_id=snowflake_conn_id,
                sql_path=medlist_script,
                render_kwargs=kwargs,
            )
                

            with TaskGroup("sites") as sites:
                # NOTE:
                # PUT requires the worker to have access to the local filesystem path.
                # This matches your existing behavior; if workers are remote, you’ll want an external stage instead.
                metadata_schema = kwargs["metadata_schema"]
                local_stage = f"file://{sites_root}/{cfg.site}"
                tsv_stage = "i2b2_ont_import_tsv"
                tsv_format = "TSV_FORMAT"
                put_params = "PARALLEL=4 AUTO_COMPRESS=TRUE SOURCE_COMPRESSION=AUTO_DETECT OVERWRITE=TRUE"

                create_format_sql = f"""
                    CREATE OR REPLACE FILE FORMAT {metadata_schema}.{tsv_format}
                    TYPE = CSV
                    FIELD_DELIMITER = '\t'
                    ESCAPE = NONE
                    NULL_IF = ('NULL')
                    COMPRESSION = AUTO
                    ESCAPE_UNENCLOSED_FIELD = NONE
                    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                    SKIP_HEADER = 1;
                """.strip()

                create_stage_sql = f"""
                    CREATE OR REPLACE STAGE {metadata_schema}.{tsv_stage}
                    FILE_FORMAT = {tsv_format};
                """.strip()

                put_sql = f"""
                    PUT {local_stage}/*.tsv @{metadata_schema}.{tsv_stage} {put_params};
                """.strip()

                load_sql = f"""
                    USE SCHEMA {metadata_schema};
                    DELETE FROM ACT_DEM_V41
                    WHERE C_FULLNAME LIKE '%\\GPC Sites\\%' OR C_FULLNAME LIKE '%\\PCORNet Sites\\%';
                    COPY INTO ACT_DEM_V41
                    FROM @{tsv_stage}/sites.tsv
                    FILE_FORMAT = {tsv_format};
                """.strip()

                create_format = SnowflakeSqlApiOperator(
                    task_id="create_tsv_format",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=create_format_sql,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0,
                )

                create_stage = SnowflakeSqlApiOperator(
                    task_id="create_tsv_stage",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=create_stage_sql,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0,
                )

                put_files = SnowflakeSqlApiOperator(
                    task_id="upload_sites_tsv",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=put_sql,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0,
                )

                load_sites = SnowflakeSqlApiOperator(
                    task_id="load_sites",
                    snowflake_conn_id=snowflake_conn_id,
                    sql=load_sql,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    autocommit=True,
                    retries=0,
                )

                create_format >> create_stage >> put_files >> load_sites

            adi = sf_sql_task(
                    task_id="build_adi",
                    conn_id=snowflake_conn_id,
                    sql_path=adi_script,
                    render_kwargs=kwargs,
                )
                
            acs = sf_sql_task(
                    task_id="build_acs",
                    conn_id=snowflake_conn_id,
                    sql_path=acs_script,
                    render_kwargs=kwargs,
                )
                

            # Keep your exact order:
            facility_id >> facility_location >> medlist >> sites >> adi >> acs

        return dag

    except Exception:
        logger.exception("Failed to build generated ontology DAG for config: %s", cfg.env_path)
        return None


# --------- register DAGs ---------
_configs = discover_configs(BASE_ENV_DIR)
if not _configs:
    logger.warning("No env configs found under %s", BASE_ENV_DIR)

for _cfg in _configs:
    _dag = build_dag(_cfg)
    if _dag:
        globals()[_dag.dag_id] = _dag
