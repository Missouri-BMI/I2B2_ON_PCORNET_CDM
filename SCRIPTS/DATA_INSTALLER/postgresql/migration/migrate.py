from __future__ import annotations

import os
import io
import csv
from dataclasses import dataclass
from typing import List, Optional, Sequence, Set, Tuple

import pandas as pd
import snowflake.connector as sc
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv


# -------------------------------------------------
# Load .env
# -------------------------------------------------
load_dotenv()


# -------------------------------------------------
# Snowflake schemas (UPPERCASE)
# -------------------------------------------------
SCHEMAS_TO_MIGRATE = [
    "I2B2HIVE",
    "I2B2PM",
    "I2B2WORKDATA",
]


# -------------------------------------------------
# Config models
# -------------------------------------------------
@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    role: Optional[str] = None


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: Optional[str] = None


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required value in .env: {name}")
    return v


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def load_configs() -> tuple[SnowflakeConfig, PostgresConfig]:
    return (
        SnowflakeConfig(
            account=env_required("SF_ACCOUNT"),
            user=env_required("SF_USER"),
            warehouse=env_required("SF_WAREHOUSE"),
            database=env_required("SF_DATABASE"),
            role=os.getenv("SF_ROLE"),
        ),
        PostgresConfig(
            host=env_required("PG_HOST"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=env_required("PG_DATABASE"),
            user=env_required("PG_USER"),
            password=env_required("PG_PASSWORD"),
            sslmode=os.getenv("PG_SSLMODE"),
        ),
    )


# -------------------------------------------------
# Connections
# -------------------------------------------------
def sf_connect(cfg: SnowflakeConfig):
    params = {
        "account": cfg.account,
        "user": cfg.user,
        "warehouse": cfg.warehouse,
        "database": cfg.database,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": env_required("SF_PRIVATE_KEY_FILE"),
    }
    if cfg.role:
        params["role"] = cfg.role
    if os.getenv("SF_PRIVATE_KEY_PWD"):
        params["private_key_file_pwd"] = os.getenv("SF_PRIVATE_KEY_PWD")
    return sc.connect(**params)


def pg_connect(cfg: PostgresConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
        sslmode=cfg.sslmode,
    )


# -------------------------------------------------
# Discovery
# -------------------------------------------------
def list_sf_tables(
    sf_cur, database: str, schemas: Sequence[str]
) -> List[Tuple[str, str]]:
    schema_list = ", ".join(f"'{s}'" for s in schemas)
    sf_cur.execute(
        f"""
        SELECT table_schema, table_name
        FROM "{database}".INFORMATION_SCHEMA.TABLES
        WHERE table_type = 'BASE TABLE'
          AND table_schema IN ({schema_list})
        ORDER BY table_schema, table_name
        """
    )
    return [(s, t) for s, t in sf_cur.fetchall()]


def list_pg_tables(pg_cur) -> Set[Tuple[str, str]]:
    pg_cur.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        """
    )
    return {(s.lower(), t.lower()) for s, t in pg_cur.fetchall()}


# -------------------------------------------------
# Migration helpers
# -------------------------------------------------
def get_pg_columns(pg_cur, schema: str, table: str) -> List[str]:
    pg_cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema.lower(), table.lower()),
    )
    cols = [c[0] for c in pg_cur.fetchall()]
    if not cols:
        raise RuntimeError(f"Postgres table not found: {schema}.{table}")
    return cols


def fetch_sf_df(sf_cur, database: str, schema: str, table: str) -> pd.DataFrame:
    sf_cur.execute(f'SELECT * FROM "{database}"."{schema}"."{table}"')
    return sf_cur.fetch_pandas_all()


def align_df(df: pd.DataFrame, pg_columns: Sequence[str]) -> pd.DataFrame:
    sf_map = {c.lower(): c for c in df.columns}
    data = {}
    for col in pg_columns:
        src = sf_map.get(col.lower())
        data[col] = df[src] if src else pd.Series([None] * len(df))
    return pd.DataFrame(data, columns=pg_columns)


def truncate_pg(pg_cur, schema: str, table: str):
    pg_cur.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema.lower()),
            sql.Identifier(table.lower()),
        )
    )


def copy_to_pg(pg_cur, schema: str, table: str, df: pd.DataFrame) -> int:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    copy_sql = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    ).format(
        sql.Identifier(schema.lower()),
        sql.Identifier(table.lower()),
        sql.SQL(", ").join(map(sql.Identifier, df.columns)),
    )

    pg_cur.copy_expert(copy_sql.as_string(pg_cur.connection), buf)
    return len(df)


# -------------------------------------------------
# Main
# -------------------------------------------------
def migrate_i2b2_schemas():
    sf_cfg, pg_cfg = load_configs()
    truncate_before_load = env_bool("MIGRATE_TRUNCATE", True)

    with sf_connect(sf_cfg) as sf_conn, pg_connect(pg_cfg) as pg_conn:
        sf_cur = sf_conn.cursor()
        pg_cur = pg_conn.cursor()

        try:
            sf_tables = list_sf_tables(sf_cur, sf_cfg.database, SCHEMAS_TO_MIGRATE)
            pg_tables = list_pg_tables(pg_cur)

            matching = [
                (s, t)
                for s, t in sf_tables
                if (s.lower(), t.lower()) in pg_tables
            ]

            print(f"Tables to migrate: {len(matching)}")

            for sf_schema, sf_table in matching:
                pg_schema = sf_schema.lower()
                pg_table = sf_table.lower()

                print(f"\n--- {sf_schema}.{sf_table} → {pg_schema}.{pg_table} ---")

                pg_cols = get_pg_columns(pg_cur, pg_schema, pg_table)
                df = fetch_sf_df(sf_cur, sf_cfg.database, sf_schema, sf_table)
                df = align_df(df, pg_cols)

                if truncate_before_load:
                    truncate_pg(pg_cur, pg_schema, pg_table)

                rows = copy_to_pg(pg_cur, pg_schema, pg_table, df)
                pg_conn.commit()

                print(f"Inserted {rows:,} rows")

        except Exception:
            pg_conn.rollback()
            raise
        finally:
            sf_cur.close()
            pg_cur.close()


if __name__ == "__main__":
    migrate_i2b2_schemas()
