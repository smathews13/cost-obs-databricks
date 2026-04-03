"""Lakebase (PostgreSQL) connection management for persistent app state.

Lakebase is a Databricks-managed PostgreSQL service. It's used here as the
primary storage for app configuration (permissions, etc.) because it persists
across deployments and doesn't require Delta/Unity Catalog table permissions.

Setup:
1. Create a Lakebase Autoscaling project in your Databricks workspace
2. Add it as a resource to the app: Configure → Add resource → Database
3. Set the env vars below in app.yaml (Databricks injects them automatically
   when you add the Lakebase resource in the app UI)
"""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# These env vars are injected automatically when a Lakebase resource is added
# to the app in the Databricks Apps UI.
# PGUSER is optional — derived from DATABRICKS_CLIENT_ID if not set explicitly
_REQUIRED_ENV_VARS = ["PGHOST", "PGDATABASE", "ENDPOINT_NAME"]

_pool: Any = None  # psycopg_pool.ConnectionPool — None if Lakebase not configured


def _get_pool():
    """Get or create the psycopg connection pool.

    Returns None if Lakebase env vars are not set, so the app degrades
    gracefully to Delta table / local file storage.
    """
    global _pool
    if _pool is not None:
        return _pool

    if not all(os.getenv(v) for v in _REQUIRED_ENV_VARS):
        return None

    try:
        import psycopg
        from psycopg_pool import ConnectionPool
        from server.db import get_workspace_client

        endpoint_name = os.environ["ENDPOINT_NAME"]

        class OAuthConnection(psycopg.Connection):
            """Injects a fresh OAuth token before every new connection."""

            @classmethod
            def connect(cls, conninfo: str = "", **kwargs: Any):
                w = get_workspace_client()
                cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
                kwargs["password"] = cred.token
                return super().connect(conninfo, **kwargs)

        # PGUSER: explicit env var, or fall back to the app service principal's
        # client_id (DATABRICKS_CLIENT_ID is auto-injected by Databricks Apps).
        pg_user = (
            os.getenv("PGUSER")
            or os.getenv("DATABRICKS_CLIENT_ID")
            or get_workspace_client().config.client_id
            or ""
        )

        conninfo = (
            f"dbname={os.environ['PGDATABASE']} "
            f"user={pg_user} "
            f"host={os.environ['PGHOST']} "
            f"port={os.getenv('PGPORT', '5432')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')}"
        )

        _pool = ConnectionPool(
            conninfo=conninfo,
            connection_class=OAuthConnection,
            min_size=1,
            max_size=5,
            open=True,
        )
        logger.info("Lakebase connection pool initialized")
        return _pool

    except Exception as e:
        logger.warning(f"Could not initialize Lakebase connection pool: {e}")
        return None


def is_available() -> bool:
    """Return True if Lakebase is configured and the pool is healthy."""
    return _get_pool() is not None


def _ensure_permissions_table(conn) -> None:
    """Create the app_user_permissions table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_user_permissions (
                role    TEXT NOT NULL,
                email   TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)


def save_permissions(admins: list[str], consumers: list[str]) -> None:
    """Persist permissions to Lakebase. Raises if Lakebase is not configured."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured (missing PGHOST / ENDPOINT_NAME env vars)")

    with pool.connection() as conn:
        _ensure_permissions_table(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_user_permissions")
            for email in admins:
                cur.execute(
                    "INSERT INTO app_user_permissions (role, email) VALUES (%s, %s)",
                    ("admin", email),
                )
            for email in consumers:
                cur.execute(
                    "INSERT INTO app_user_permissions (role, email) VALUES (%s, %s)",
                    ("consumer", email),
                )
        conn.commit()

    logger.info(f"Permissions saved to Lakebase ({len(admins)} admins, {len(consumers)} consumers)")


def load_permissions() -> dict | None:
    """Load permissions from Lakebase. Returns None if unavailable or empty."""
    pool = _get_pool()
    if pool is None:
        return None

    try:
        with pool.connection() as conn:
            _ensure_permissions_table(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT role, email FROM app_user_permissions")
                rows = cur.fetchall()
            conn.commit()

        admins = [r[1] for r in rows if r[0] == "admin"]
        consumers = [r[1] for r in rows if r[0] == "consumer"]

        if admins or consumers:
            logger.info(f"Loaded permissions from Lakebase ({len(admins)} admins, {len(consumers)} consumers)")
            return {"admins": admins, "consumers": consumers}

        return None  # table exists but is empty — no permissions set yet

    except Exception as e:
        logger.error(f"Could not load permissions from Lakebase: {e}")
        return None


# ── Table DDL ─────────────────────────────────────────────────────────────────

_DDL_STATEMENTS = [
    # Permissions (already created on demand in _ensure_permissions_table,
    # included here so _ensure_all_tables covers it)
    """
    CREATE TABLE IF NOT EXISTS app_user_permissions (
        role       TEXT NOT NULL,
        email      TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Use cases
    """
    CREATE TABLE IF NOT EXISTS use_cases (
        use_case_id TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        description TEXT,
        owner       TEXT,
        tags        JSONB,
        created_at  TIMESTAMPTZ NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL,
        status      TEXT NOT NULL DEFAULT 'active',
        stage       TEXT,
        start_date  DATE,
        end_date    DATE,
        live_date   DATE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_use_cases_status ON use_cases(status)
    """,
    # Use case objects
    """
    CREATE TABLE IF NOT EXISTS use_case_objects (
        mapping_id       TEXT PRIMARY KEY,
        use_case_id      TEXT NOT NULL,
        object_type      TEXT NOT NULL,
        object_id        TEXT NOT NULL,
        object_name      TEXT,
        workspace_id     TEXT,
        assigned_at      TIMESTAMPTZ NOT NULL,
        assigned_by      TEXT,
        notes            TEXT,
        custom_start_date DATE,
        custom_end_date   DATE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_uco_use_case_id ON use_case_objects(use_case_id)
    """,
    # ── Materialized views ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS daily_usage_summary (
        usage_date       DATE    NOT NULL,
        total_dbus       FLOAT8,
        total_spend      FLOAT8,
        workspace_count  BIGINT,
        PRIMARY KEY (usage_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_product_breakdown (
        usage_date       DATE    NOT NULL,
        product_category TEXT    NOT NULL,
        total_dbus       FLOAT8,
        total_spend      FLOAT8,
        workspace_count  BIGINT,
        PRIMARY KEY (usage_date, product_category)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_workspace_breakdown (
        usage_date   DATE   NOT NULL,
        workspace_id TEXT   NOT NULL,
        total_dbus   FLOAT8,
        total_spend  FLOAT8,
        PRIMARY KEY (usage_date, workspace_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_etl_breakdown (
        usage_date  DATE   NOT NULL,
        etl_type    TEXT   NOT NULL,
        total_dbus  FLOAT8,
        total_spend FLOAT8,
        PRIMARY KEY (usage_date, etl_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_sku_breakdown (
        usage_date       DATE   NOT NULL,
        product          TEXT   NOT NULL,
        workspaces_using BIGINT,
        total_dbus       FLOAT8,
        total_spend      FLOAT8,
        PRIMARY KEY (usage_date, product)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sql_tool_attribution (
        sql_product      TEXT   NOT NULL,
        usage_date       DATE   NOT NULL,
        warehouse_id     TEXT   NOT NULL,
        attributed_dbus  FLOAT8,
        attributed_spend FLOAT8,
        PRIMARY KEY (sql_product, usage_date, warehouse_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_query_stats (
        usage_date           DATE   NOT NULL PRIMARY KEY,
        total_queries        BIGINT,
        unique_query_users   BIGINT,
        total_rows_read      BIGINT,
        total_bytes_read     BIGINT,
        total_compute_seconds FLOAT8
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dbsql_cost_per_query (
        statement_id                        TEXT PRIMARY KEY,
        query_source_id                     TEXT,
        query_source_type                   TEXT,
        client_application                  TEXT,
        executed_by                         TEXT,
        warehouse_id                        TEXT,
        statement_text                      TEXT,
        workspace_id                        TEXT,
        start_time                          TIMESTAMPTZ,
        end_time                            TIMESTAMPTZ,
        duration_seconds                    BIGINT,
        query_attributed_dollars_estimation FLOAT8,
        query_attributed_dbus_estimation    FLOAT8,
        query_profile_url                   TEXT,
        url_helper                          TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dcpq_start_time ON dbsql_cost_per_query(start_time DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS dbsql_cost_per_query_prpr (
        statement_id                        TEXT PRIMARY KEY,
        query_source_id                     TEXT,
        query_source_type                   TEXT,
        client_application                  TEXT,
        executed_by                         TEXT,
        warehouse_id                        TEXT,
        statement_text                      TEXT,
        workspace_id                        TEXT,
        start_time                          TIMESTAMPTZ,
        end_time                            TIMESTAMPTZ,
        duration_seconds                    BIGINT,
        query_attributed_dollars_estimation FLOAT8,
        query_attributed_dbus_estimation    FLOAT8,
        query_profile_url                   TEXT,
        url_helper                          TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dcpqp_start_time ON dbsql_cost_per_query_prpr(start_time DESC)
    """,
    # Customer discounts
    """
    CREATE TABLE IF NOT EXISTS customer_discounts (
        id             SERIAL PRIMARY KEY,
        sku_name       TEXT NOT NULL,
        cloud          TEXT NOT NULL DEFAULT '*',
        discount_type  TEXT NOT NULL CHECK (discount_type IN ('multiplier', 'fixed_rate')),
        discount_value FLOAT8 NOT NULL,
        valid_from     DATE,
        valid_to       DATE,
        notes          TEXT,
        created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # ── Schema migrations: add effective_list_spend to existing MV tables ────────
    "ALTER TABLE daily_usage_summary ADD COLUMN IF NOT EXISTS effective_list_spend FLOAT8",
    "ALTER TABLE daily_product_breakdown ADD COLUMN IF NOT EXISTS effective_list_spend FLOAT8",
    "ALTER TABLE daily_workspace_breakdown ADD COLUMN IF NOT EXISTS effective_list_spend FLOAT8",
    "ALTER TABLE daily_etl_breakdown ADD COLUMN IF NOT EXISTS effective_list_spend FLOAT8",
    "ALTER TABLE daily_sku_breakdown ADD COLUMN IF NOT EXISTS effective_list_spend FLOAT8",
    "ALTER TABLE sql_tool_attribution ADD COLUMN IF NOT EXISTS attributed_effective_list_spend FLOAT8",
    "ALTER TABLE dbsql_cost_per_query ADD COLUMN IF NOT EXISTS query_attributed_dollars_effective FLOAT8",
    "ALTER TABLE dbsql_cost_per_query_prpr ADD COLUMN IF NOT EXISTS query_attributed_dollars_effective FLOAT8",
]


def _ensure_all_tables() -> None:
    """Create all Lakebase app tables if they don't exist (idempotent)."""
    pool = _get_pool()
    if pool is None:
        return
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for ddl in _DDL_STATEMENTS:
                cur.execute(ddl)
        conn.commit()
    logger.info("Lakebase: all app tables ensured")


# ── Generic helpers ───────────────────────────────────────────────────────────

def execute_pg(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    """Run a SELECT against Lakebase and return rows as dicts.

    Returns None if Lakebase is not configured.
    Raises on query error so callers can fall back to Delta.
    """
    pool = _get_pool()
    if pool is None:
        return None  # type: ignore[return-value]
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


_BATCH_SIZE = 500


def execute_pg_mv(
    delta_sql_template: str,
    params: dict,
    catalog: str,
    schema: str,
) -> list[dict[str, Any]] | None:
    """Run a Delta-style MV query against Lakebase.

    Converts {catalog}.{schema}.table → table and :param → %(param)s.
    Returns None if Lakebase not configured. Raises on SQL error so
    callers can fall back to Delta.
    """
    pool = _get_pool()
    if pool is None:
        return None

    # Substitute catalog/schema placeholders, then strip the prefix
    try:
        sql = delta_sql_template.format(catalog=catalog, schema=schema)
    except (KeyError, IndexError):
        sql = delta_sql_template

    sql = sql.replace(f"{catalog}.{schema}.", "")
    # Convert :param_name → %(param_name)s  (skip ::type casts)
    sql = _re.sub(r"(?<!:):([A-Za-z_]\w*)", r"%(\1)s", sql)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def lakebase_mv_available() -> bool:
    """Return True if Lakebase is up and has the core MV tables populated."""
    pool = _get_pool()
    if pool is None:
        return False
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM daily_usage_summary")
                count = cur.fetchone()[0]
        return count > 0
    except Exception:
        return False


def bulk_replace_table(table_name: str, columns: list[str], rows: list[tuple]) -> None:
    """TRUNCATE a Lakebase table and bulk-insert new rows.

    Uses batched executemany to avoid memory spikes on large tables.
    Raises if Lakebase is not configured.
    """
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured")

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name}")
            for i in range(0, len(rows), _BATCH_SIZE):
                batch = rows[i : i + _BATCH_SIZE]
                cur.executemany(insert_sql, batch)
        conn.commit()

    logger.info(f"Lakebase: bulk_replace_table {table_name} — {len(rows)} rows")


# ── Use Cases ─────────────────────────────────────────────────────────────────

import json as _json
import re as _re
from datetime import date as _date, datetime as _dt


def _coerce_date(v: Any) -> Any:
    """Convert date/datetime/string to date or None."""
    if v is None:
        return None
    if isinstance(v, _date):
        return v
    if isinstance(v, str) and v:
        try:
            return _dt.strptime(v[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _coerce_ts(v: Any) -> Any:
    """Convert to datetime string or None."""
    if v is None:
        return None
    if isinstance(v, (_dt,)):
        return v
    if isinstance(v, str) and v:
        return v
    return None


def save_use_case(uc: dict[str, Any]) -> None:
    """Upsert a use case into Lakebase. Raises if unavailable."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured")

    tags = uc.get("tags")
    if isinstance(tags, dict):
        tags = _json.dumps(tags)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO use_cases
                    (use_case_id, name, description, owner, tags, created_at, updated_at,
                     status, stage, start_date, end_date, live_date)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (use_case_id) DO UPDATE SET
                    name        = EXCLUDED.name,
                    description = EXCLUDED.description,
                    owner       = EXCLUDED.owner,
                    tags        = EXCLUDED.tags,
                    updated_at  = EXCLUDED.updated_at,
                    status      = EXCLUDED.status,
                    stage       = EXCLUDED.stage,
                    start_date  = EXCLUDED.start_date,
                    end_date    = EXCLUDED.end_date,
                    live_date   = EXCLUDED.live_date
            """, (
                uc["use_case_id"],
                uc.get("name"),
                uc.get("description"),
                uc.get("owner"),
                tags,
                _coerce_ts(uc.get("created_at")),
                _coerce_ts(uc.get("updated_at")),
                uc.get("status", "active"),
                uc.get("stage"),
                _coerce_date(uc.get("start_date")),
                _coerce_date(uc.get("end_date")),
                _coerce_date(uc.get("live_date")),
            ))
        conn.commit()


def load_use_cases(status: str | None = None, stage: str | None = None) -> list[dict[str, Any]] | None:
    """Load use cases from Lakebase. Returns None if unavailable."""
    pool = _get_pool()
    if pool is None:
        return None
    try:
        conditions = []
        params: list[Any] = []
        if status:
            conditions.append("status = %s")
            params.append(status)
        if stage:
            conditions.append("stage = %s")
            params.append(stage)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM use_cases {where} ORDER BY created_at DESC"
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        # Convert tags JSONB (already dict from psycopg) and dates to strings
        for row in rows:
            if row.get("tags") is None:
                row["tags"] = {}
            for f in ("created_at", "updated_at", "start_date", "end_date", "live_date"):
                v = row.get(f)
                row[f] = str(v) if v is not None else None
        return rows
    except Exception as e:
        logger.error(f"Lakebase load_use_cases failed: {e}")
        return None


def delete_use_case(use_case_id: str) -> None:
    """Delete a use case and its objects from Lakebase. Raises if unavailable."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM use_case_objects WHERE use_case_id = %s", (use_case_id,))
            cur.execute("DELETE FROM use_cases WHERE use_case_id = %s", (use_case_id,))
        conn.commit()


# ── Use Case Objects ──────────────────────────────────────────────────────────

def save_use_case_object(obj: dict[str, Any]) -> None:
    """Upsert a use case object mapping into Lakebase. Raises if unavailable."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO use_case_objects
                    (mapping_id, use_case_id, object_type, object_id, object_name,
                     workspace_id, assigned_at, assigned_by, notes,
                     custom_start_date, custom_end_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (mapping_id) DO UPDATE SET
                    object_name       = EXCLUDED.object_name,
                    workspace_id      = EXCLUDED.workspace_id,
                    assigned_by       = EXCLUDED.assigned_by,
                    notes             = EXCLUDED.notes,
                    custom_start_date = EXCLUDED.custom_start_date,
                    custom_end_date   = EXCLUDED.custom_end_date
            """, (
                obj["mapping_id"],
                obj["use_case_id"],
                obj.get("object_type"),
                obj.get("object_id"),
                obj.get("object_name"),
                obj.get("workspace_id"),
                _coerce_ts(obj.get("assigned_at")),
                obj.get("assigned_by"),
                obj.get("notes"),
                _coerce_date(obj.get("custom_start_date")),
                _coerce_date(obj.get("custom_end_date")),
            ))
        conn.commit()


def load_use_case_objects(use_case_id: str) -> list[dict[str, Any]] | None:
    """Load objects for a use case from Lakebase. Returns None if unavailable."""
    pool = _get_pool()
    if pool is None:
        return None
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM use_case_objects WHERE use_case_id = %s ORDER BY assigned_at DESC",
                    (use_case_id,),
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        for row in rows:
            for f in ("assigned_at", "custom_start_date", "custom_end_date"):
                v = row.get(f)
                row[f] = str(v) if v is not None else None
        return rows
    except Exception as e:
        logger.error(f"Lakebase load_use_case_objects failed: {e}")
        return None


def delete_use_case_object(mapping_id: str) -> None:
    """Delete a use case object mapping from Lakebase. Raises if unavailable."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM use_case_objects WHERE mapping_id = %s", (mapping_id,))
        conn.commit()


# ── Customer Discounts ────────────────────────────────────────────────────────

def save_discount(discount: dict[str, Any]) -> dict[str, Any]:
    """Upsert a customer discount. Returns the saved row with its id."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured — discounts require Lakebase")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            if discount.get("id"):
                cur.execute("""
                    UPDATE customer_discounts
                    SET sku_name=%s, cloud=%s, discount_type=%s, discount_value=%s,
                        valid_from=%s, valid_to=%s, notes=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                    RETURNING id, sku_name, cloud, discount_type, discount_value,
                              valid_from, valid_to, notes, created_at, updated_at
                """, (
                    discount["sku_name"], discount.get("cloud", "*"),
                    discount["discount_type"], float(discount["discount_value"]),
                    _coerce_date(discount.get("valid_from")),
                    _coerce_date(discount.get("valid_to")),
                    discount.get("notes"),
                    discount["id"],
                ))
            else:
                cur.execute("""
                    INSERT INTO customer_discounts
                        (sku_name, cloud, discount_type, discount_value, valid_from, valid_to, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, sku_name, cloud, discount_type, discount_value,
                              valid_from, valid_to, notes, created_at, updated_at
                """, (
                    discount["sku_name"], discount.get("cloud", "*"),
                    discount["discount_type"], float(discount["discount_value"]),
                    _coerce_date(discount.get("valid_from")),
                    _coerce_date(discount.get("valid_to")),
                    discount.get("notes"),
                ))
            cols = [d[0] for d in cur.description]
            row = dict(zip(cols, cur.fetchone()))
        conn.commit()
    for f in ("valid_from", "valid_to", "created_at", "updated_at"):
        row[f] = str(row[f]) if row.get(f) is not None else None
    return row


def load_discounts(cloud: str | None = None) -> list[dict[str, Any]] | None:
    """Load all customer discounts. Returns None if Lakebase unavailable."""
    pool = _get_pool()
    if pool is None:
        return None
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if cloud:
                    cur.execute(
                        "SELECT * FROM customer_discounts WHERE cloud = %s OR cloud = '*' ORDER BY sku_name",
                        (cloud,),
                    )
                else:
                    cur.execute("SELECT * FROM customer_discounts ORDER BY sku_name, cloud")
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        for row in rows:
            for f in ("valid_from", "valid_to", "created_at", "updated_at"):
                row[f] = str(row[f]) if row.get(f) is not None else None
        return rows
    except Exception as e:
        logger.error(f"Lakebase load_discounts failed: {e}")
        return None


def delete_discount(discount_id: int) -> None:
    """Delete a customer discount by id. Raises if Lakebase unavailable."""
    pool = _get_pool()
    if pool is None:
        raise RuntimeError("Lakebase is not configured — discounts require Lakebase")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customer_discounts WHERE id = %s", (discount_id,))
        conn.commit()


def apply_discounts_to_rows(
    rows: list[dict[str, Any]],
    spend_field: str,
    sku_field: str,
    cloud: str | None = None,
) -> list[dict[str, Any]]:
    """Apply customer discounts to a list of rows in-place.

    Adds a `{spend_field}_list_price` key preserving the original value.
    Falls back to the original value if no matching discount exists.
    """
    from datetime import date as _date
    discounts = load_discounts(cloud)
    if not discounts:
        return rows

    today = _date.today()
    lookup: dict[tuple[str, str], dict] = {}
    for d in discounts:
        vf = d.get("valid_from")
        vt = d.get("valid_to")
        if vf and str(vf) > today.isoformat():
            continue
        if vt and str(vt) < today.isoformat():
            continue
        key = (d["sku_name"].upper(), d["cloud"].upper())
        lookup[key] = d

    for row in rows:
        sku = (row.get(sku_field) or "").upper()
        cl = (cloud or "").upper()
        discount = lookup.get((sku, cl)) or lookup.get((sku, "*"))
        if not discount:
            continue
        orig = float(row.get(spend_field) or 0)
        row[f"{spend_field}_list_price"] = orig
        if discount["discount_type"] == "multiplier":
            row[spend_field] = orig * float(discount["discount_value"])
        else:  # fixed_rate: $/DBU override
            dbus = float(row.get("total_dbus") or 0)
            row[spend_field] = dbus * float(discount["discount_value"])
    return rows
