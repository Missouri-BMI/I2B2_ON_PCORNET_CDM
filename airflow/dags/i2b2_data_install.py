import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import pendulum
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.bash import BashOperator
from dotenv import dotenv_values

from common import *

logger = logging.getLogger(__name__)

# --------- constants / defaults ---------
BASE_ENV_DIR = os.getenv("I2B2_ENV_BASE_DIR", "/opt/airflow/env")
BASE_PATH = os.getenv("I2B2_BASE_PATH", "/opt/airflow/SCRIPTS/DATA_INSTALLER")

DATA_INSTALLER_PATH = f"{BASE_PATH}/i2b2-data"
ANT_PATH = f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/apache-ant/bin/ant"
ANT_BUILD_PATH = f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/NewInstall/build.xml"

DEFAULT_SCHEDULE = os.getenv("I2B2_DATA_INSTALL_SCHEDULE", None)
DEFAULT_START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

# Available in all Environment
FILTER_ACCOUNTS = set(x.strip() for x in os.getenv("I2B2_FILTER_ACCOUNTS", "deidentified").split(",") if x.strip())
FILTER_ENVS = set(x.strip() for x in os.getenv("I2B2_FILTER_ENVS", "dev, prod, sandbox").split(",") if x.strip())
FILTER_SITES = set(x.strip() for x in os.getenv("I2B2_FILTER_SITES", "mu, gpc, shrine-mu, shrine-washu").split(",") if x.strip())

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

    crc_schema = f"{target_db}.{a['CRC_SCHEMA']}"
    hive_schema = f"{target_db}.{a['HIVE_SCHEMA']}"
    pm_schema = f"{target_db}.{a['PM_SCHEMA']}"
    metadata_schema = f"{target_db}.{a['METADATA_SCHEMA']}"
    wd_schema = f"{target_db}.{a['WORKDATA_SCHEMA']}"

    project_db = a["PROJECT_DB"]
    project_pm = f"{project_db}.{a['PM_SCHEMA']}"
    project_hive = f"{project_db}.{a['HIVE_SCHEMA']}"

    stage_schema = f"{target_db}.enact_stage"

    kwargs = {
        "crc_schema": crc_schema,
        "hive_schema": hive_schema,
        "metadata_schema": metadata_schema,
        "pm_schema": pm_schema,
        "wd_schema": wd_schema,
        "stage_schema": stage_schema,
        "source_schema": source_schema,
        "target_schema": target_schema,
        "target_db": target_db,
        "project_pm": project_pm,
        "project_hive": project_hive,
        "site": effective_site
    }
    return kwargs


def build_dag(cfg: RunConfig) -> Optional[DAG]:
    """
    Build one DAG per config for i2b2 data installation.
    """
    try:
        a = cfg.values
        snowflake_conn_id = a["CONNECTION_ID"]

        dag_id = f"i2b2_data_install__{cfg.account}__{cfg.environment}__{cfg.site}"
        schedule = a.get("SCHEDULE", DEFAULT_SCHEDULE)

        dag = DAG(
            dag_id=dag_id,
            description=f"i2b2 data loader in Snowflake ({cfg.account}/{cfg.environment}/{cfg.site})",
            schedule=schedule,
            start_date=DEFAULT_START_DATE,
            catchup=False,
            max_active_runs=1,
            tags=["i2b2_data_install", cfg.account, cfg.environment, cfg.site],
            default_args={
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
            },
        )

        render_kwargs = _build_kwargs(cfg)

        with dag:
            # 1) create i2b2 schemas
            with TaskGroup("create_i2b2_schema") as init:
                create_schema_sql_path = f"{BASE_PATH}/CONFIGURE/COMMON/create_schema.sql"
                sf_sql_task(
                    task_id="create_schema_task",
                    conn_id=snowflake_conn_id,
                    sql_path=create_schema_sql_path,
                    render_kwargs=render_kwargs
                )
            

            # 2) run ANT installer (create_database + load_demodata)
            i2b2_data = BashOperator(
                task_id="i2b2_data",
                bash_command=f'{ANT_PATH} -f {ANT_BUILD_PATH} create_database load_demodata',
                retries=0,
            )

            # 3) common configure SQL directory (sequential)
            common_dir = f"{BASE_PATH}/CONFIGURE/COMMON/SERVICES"
            common_cfg = make_sql_chain_in_dir(
                group_id="i2b2_common_configure",
                sql_dir=common_dir,
                snowflake_conn_id=snowflake_conn_id,
                render_kwargs=render_kwargs,
            )

            # 4) project configure SQL directory (sequential)
            project_dir = f"{BASE_PATH}/CONFIGURE/PROJECT"
            project_cfg = make_sql_chain_in_dir(
                group_id="i2b2_project_configure",
                sql_dir=project_dir,
                snowflake_conn_id=snowflake_conn_id,
                render_kwargs=render_kwargs,
            )

            init >> i2b2_data >> common_cfg >> project_cfg

        return dag

    except Exception:
        logger.exception("Failed to build i2b2 DAG for config: %s", cfg.env_path)
        return None


# --------- register DAGs ---------
_configs = discover_configs(BASE_ENV_DIR)
if not _configs:
    logger.warning("No i2b2 env configs found under %s", BASE_ENV_DIR)

for _cfg in _configs:
    _dag = build_dag(_cfg)
    if _dag:
        globals()[_dag.dag_id] = _dag
