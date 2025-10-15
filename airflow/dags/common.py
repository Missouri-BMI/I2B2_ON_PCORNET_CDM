from pathlib import Path
from datetime import datetime, timedelta
import json, os, logging
from jinja2 import Template
from typing import Dict, List, Optional
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

def get_task_id(file_path):
    filename = Path(file_path).stem
    return f'execute_{filename.split(".")[0]}'   

def read_sql_from_file(file_path: str, **kwargs) -> str:
    # Read SQL file from the specified directory
    sql_path = Path(file_path)
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    # Read the content of the SQL file
    with open(sql_path, 'r') as sql_file:
        sql_content = sql_file.read()

    if not sql_content.strip():
        raise ValueError(f"SQL file {file_path} is empty")

    # Apply Jinja2 templating
    template = Template(sql_content)
    rendered_sql = template.render(**kwargs)
    return rendered_sql

def resolve_site(account: str, site: str) -> str:
    """
    Resolve the effective site identifier used everywhere (SQL context + table_mapping.json key).

    Rules:
      - deidentified -> mu, gpc, shrine-washu (no suffix)
    """
    if account == "deidentified" and site.startswith("shrine-"):
        return site.split("-")[1]
    return site


def extract_table_mapping_from_file(filename, project_key):
    with open(filename, 'r') as f:
        data = json.load(f)
    
    return {
        table: values.get(project_key)
        for table, values in data.get("tables", {}).items()
        if project_key in values
    }

def sf_sql_task(
    task_id: str,
    conn_id: str,
    sql_path: str,
    render_kwargs: Dict[str, str],
) -> SnowflakeSqlApiOperator:
    rendered = read_sql_from_file(sql_path, **render_kwargs)
    return SnowflakeSqlApiOperator(
        task_id=task_id,
        snowflake_conn_id=conn_id,
        sql=rendered,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        autocommit=True,
        retries=0,
    )


def make_sql_chain_in_dir(
    group_id: str,
    sql_dir: str,
    snowflake_conn_id: str,
    render_kwargs: Dict[str, str],
) -> TaskGroup:
    """
    Build sequential SnowflakeSqlApiOperator tasks for all SQL files in a directory.
    """
    tg = TaskGroup(group_id)
    with tg:
        prev = None
        for filename in _list_sql_files(sql_dir):
            path = os.path.join(sql_dir, filename)
            rendered = read_sql_from_file(path, **render_kwargs)

            task = SnowflakeSqlApiOperator(
                task_id=get_task_id(path),
                snowflake_conn_id=snowflake_conn_id,
                sql=rendered,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                autocommit=True,
                retries=0,
            )
            if prev:
                prev >> task
            prev = task

    return tg

def _list_sql_files(sql_dir: str) -> List[str]:
    """Return filenames (*.sql) sorted. If directory missing/empty, return empty (don't fail at parse time)."""
    if not os.path.isdir(sql_dir):
        logger.warning("SQL directory does not exist: %s", sql_dir)
        return []
    files = [f for f in sorted(os.listdir(sql_dir)) if f.endswith(".sql")]
    if not files:
        logger.warning("No .sql files found in directory: %s", sql_dir)
    return files