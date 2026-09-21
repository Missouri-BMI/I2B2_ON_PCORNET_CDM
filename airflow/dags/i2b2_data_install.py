import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import pendulum
from airflow.sdk import DAG, TaskGroup, TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.standard.operators.bash import BashOperator
from dotenv import dotenv_values

from common import *

logger = logging.getLogger(__name__)

# --------- constants / defaults ---------
BASE_ENV_DIR = os.getenv("I2B2_ENV_BASE_DIR", "/opt/airflow/env")
BASE_PATH = os.getenv("I2B2_BASE_PATH", "/opt/airflow/SCRIPTS/DATA_INSTALLER")

DATA_INSTALLER_PATH = f"{BASE_PATH}/i2b2-data"
I2B2_SNOWFLAKE_DIR = f"{DATA_INSTALLER_PATH}/docker/i2b2-snowflake"
I2B2_SNOWFLAKE_SCRIPT = f"{I2B2_SNOWFLAKE_DIR}/create_snowflake_image.sh"

DEFAULT_SCHEDULE = os.getenv("I2B2_DATA_INSTALL_SCHEDULE", None)
DEFAULT_START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

# Available in all Environment
FILTER_ACCOUNTS = set(x.strip() for x in os.getenv("I2B2_FILTER_ACCOUNTS", "deidentified").split(",") if x.strip())
FILTER_ENVS = set(x.strip() for x in os.getenv("I2B2_FILTER_ENVS", "sandbox").split(",") if x.strip())
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
            

            # 2) load i2b2 data via the i2b2-snowflake installer script
            #    (docker/i2b2-snowflake) instead of the ant create_database /
            #    load_demodata targets. Snowflake connection details (account,
            #    user, role, warehouse, key-pair file) are pulled from the Airflow
            #    connection; the target database is the config's TARGET_DB.
            i2b2_data = BashOperator(
                task_id="i2b2_data",
                # Trailing space is required: a bash_command ending in ".sh" is
                # otherwise treated by Jinja as a template file to load.
                bash_command=f"bash {I2B2_SNOWFLAKE_SCRIPT} ",
                env={
                    "SNOWFLAKE_ACCOUNT": f"{{{{ conn.{snowflake_conn_id}.extra_dejson.account }}}}",
                    "I2B2_USER": f"{{{{ conn.{snowflake_conn_id}.login }}}}",
                    "I2B2_ROLE": f"{{{{ conn.{snowflake_conn_id}.extra_dejson.role }}}}",
                    "I2B2_WAREHOUSE": f"{{{{ conn.{snowflake_conn_id}.extra_dejson.warehouse }}}}",
                    "I2B2_PRIVATE_KEY_FILE": f"{{{{ conn.{snowflake_conn_id}.extra_dejson.private_key_file }}}}",
                    # The env-dir key is encrypted; the Snowflake provider uses the
                    # connection password as the private-key passphrase, so reuse it.
                    "I2B2_PRIVATE_KEY_PWD": f"{{{{ conn.{snowflake_conn_id}.password }}}}",
                    "I2B2_DB": a["TARGET_DB"],
                },
                append_env=True,
                retries=0,
            )

            # NOTE: the common and project configure SQL (CONFIGURE/COMMON/SERVICES
            # and CONFIGURE/PROJECT) has been split out into the separate
            # `i2b2_project_configure` DAG. This DAG only performs the sandbox-only
            # base install: create schemas, load the i2b2 data, then clean up.

            # 3) clean up installer artifacts. The i2b2-snowflake script
            #    overwrites the per-cell db.properties, sed-edits the pm_access
            #    SQL, and unzips the crcdata/metadata archives into the module
            #    script dirs. Restore the tracked files and delete the extracted
            #    (untracked) ones so the i2b2-data submodule is left pristine.
            #    Runs regardless of upstream success/failure (ALL_DONE).
            newinstall_dir = f"{DATA_INSTALLER_PATH}/edu.harvard.i2b2.data/Release_1-8/NewInstall"
            cleanup = BashOperator(
                task_id="cleanup_installer_artifacts",
                bash_command=(
                    "set -e\n"
                    f"git -C '{DATA_INSTALLER_PATH}' -c safe.directory='*' checkout -- "
                    f"'{newinstall_dir}'\n"
                    f"git -C '{DATA_INSTALLER_PATH}' -c safe.directory='*' clean -fdq -- "
                    f"'{newinstall_dir}/Crcdata/act/scripts/snowflake' "
                    f"'{newinstall_dir}/Metadata/act/scripts/snowflake'"
                ),
                trigger_rule=TriggerRule.ALL_DONE,
                retries=0,
            )

            init >> i2b2_data >> cleanup

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
