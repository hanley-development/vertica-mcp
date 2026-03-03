import json
import os
import re
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import jpype
import jpype.imports

JDBC_JAR = os.environ.get("VERTICA_JDBC_JAR", "/app/drivers/vertica-jdbc.jar")
HOST = os.environ.get("VERTICA_HOST")
PORT = os.environ.get("VERTICA_PORT", "5433")
DB = os.environ.get("VERTICA_DB")
USER = os.environ.get("VERTICA_USER")
PASS = os.environ.get("VERTICA_PASS")

SCHEMA_ALLOWLIST = [s.strip() for s in (os.environ.get("VERTICA_SCHEMA_ALLOWLIST", "")).split(",") if s.strip()]

EXPORT_ROOT = Path(os.environ.get("VERTICA_EXPORT_ROOT", "/app/exports")).resolve()
WORKSPACE_ROOT = Path(os.environ.get("VERTICA_WORKSPACE_ROOT", "/app/workspace")).resolve()
AUDIT_LOG = Path(os.environ.get("VERTICA_AUDIT_LOG", "/app/audit/vertica-audit.jsonl")).resolve()

APPROVAL_MODE = (os.environ.get("VERTICA_APPROVAL_MODE", "off").strip().lower() in ("1","true","yes","on"))

if not all([HOST, DB, USER, PASS]):
    raise SystemExit("Missing env vars: VERTICA_HOST, VERTICA_DB, VERTICA_USER, VERTICA_PASS (optional VERTICA_PORT).")

JDBC_URL = f"jdbc:vertica://{HOST}:{PORT}/{DB}"

BLOCKED = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|COPY|GRANT|REVOKE)\b", re.I)
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)
AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sql_hash(sql: str) -> str:
    normalized = " ".join(sql.strip().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

def audit_log(entry: Dict[str, Any]) -> None:
    entry = dict(entry)
    entry.setdefault("ts", utc_now_iso())
    try:
        with AUDIT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass

def start_jvm() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JDBC_JAR])

def assert_readonly_sql(sql: str) -> None:
    if ";" in sql:
        raise ValueError("Multi-statement SQL is not allowed (semicolon detected).")
    s = sql.lstrip()
    if not (s.upper().startswith("SELECT") or s.upper().startswith("WITH")):
        raise ValueError("Only SELECT/WITH queries are allowed.")
    if BLOCKED.search(sql):
        raise ValueError("Blocked keyword detected (DDL/DML not allowed).")

def ensure_limit(sql: str, max_rows: int) -> str:
    if re.search(r"\bLIMIT\b", sql, re.I):
        return sql
    return f"{sql} LIMIT {max_rows}"

def assert_safe_ident(name: str, kind: str) -> str:
    if not IDENT.match(name):
        raise ValueError(f"Invalid {kind} identifier: {name}")
    return name

def assert_safe_where(where: str) -> None:
    if ";" in where:
        raise ValueError("WHERE clause cannot contain ';'.")
    w = where.strip()
    banned = re.compile(r"(--|/\*|\*/|\bSELECT\b|\bUNION\b|\bJOIN\b|\bFROM\b|\bWITH\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bMERGE\b|\bDROP\b|\bALTER\b|\bCREATE\b)", re.I)
    if banned.search(w):
        raise ValueError("WHERE clause contains a blocked keyword/pattern.")
    allowed_chars = re.compile(r"^[A-Za-z0-9_().\s=<>!,'%-]+$")
    if not allowed_chars.match(w):
        raise ValueError("WHERE clause contains unsupported characters.")

def resolve_under(root: Path, user_path: str) -> Path:
    p = Path(user_path)
    if not p.is_absolute():
        p = root / p
    resolved = p.resolve()
    try:
        resolved.relative_to(root)
    except Exception:
        raise ValueError(f"Path must be under {str(root)}")
    return resolved

def read_sql_file(path: Path, max_bytes: int = 1_000_000) -> str:
    if not path.exists() or not path.is_file():
        raise ValueError(f"SQL file not found: {str(path)}")
    size = path.stat().st_size
    if size > max_bytes:
        raise ValueError(f"SQL file too large ({size} bytes). Limit is {max_bytes}.")
    text = path.read_text(encoding="utf-8")
    return text.lstrip("\ufeff")

def single_statement(sql: str) -> str:
    s = sql.strip()
    if s.endswith(";"):
        s = s[:-1].strip()
    if ";" in s:
        raise ValueError("Multiple statements detected (semicolon).")
    return s

def jdbc_query(sql: str, params: Optional[List[Any]] = None, max_rows: int = 200) -> Dict[str, Any]:
    start_jvm()
    from java.sql import DriverManager  # type: ignore

    conn = DriverManager.getConnection(JDBC_URL, USER, PASS)
    try:
        if params:
            stmt = conn.prepareStatement(sql)
            for idx, p in enumerate(params, start=1):
                stmt.setObject(idx, p)
            stmt.setMaxRows(max_rows)
            rs = stmt.executeQuery()
        else:
            stmt = conn.createStatement()
            stmt.setMaxRows(max_rows)
            rs = stmt.executeQuery(sql)

        md = rs.getMetaData()
        col_count = md.getColumnCount()
        cols = [md.getColumnLabel(i) for i in range(1, col_count + 1)]

        rows: List[List[Any]] = []
        while rs.next() and len(rows) < max_rows:
            row: List[Any] = []
            for i in range(1, col_count + 1):
                row.append(rs.getObject(i))
            rows.append(row)

        truncated = len(rows) >= max_rows
        return {"columns": cols, "rows": rows, "row_count": len(rows), "truncated": truncated}
    finally:
        conn.close()

def to_records(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for r in rows:
        obj: Dict[str, Any] = {}
        for i, c in enumerate(columns):
            v = r[i] if i < len(r) else None
            if v is None or isinstance(v, (str, int, float, bool)):
                obj[c] = v
            else:
                obj[c] = str(v)
        recs.append(obj)
    return recs

def write_export(result: Dict[str, Any], fmt: str, out_path: Path, meta: Dict[str, Any]) -> None:
    fmt = fmt.lower().strip()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "json":
        payload = {
            "meta": meta,
            "records": result.get("records") or to_records(result.get("columns") or [], result.get("rows") or []),
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return

    if fmt == "csv":
        import csv
        cols = result.get("columns") or []
        rows = result.get("rows") or []
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for row in rows:
                w.writerow(["" if v is None else v for v in row])
        return

    raise ValueError("format must be 'csv' or 'json'")

def require_approval(args: Dict[str, Any], reason: str) -> None:
    if not APPROVAL_MODE:
        return
    if not bool(args.get("approved", False)):
        raise ValueError(f"Approval mode is ON. Reason: {reason}. Re-run with approved=true.")

TOOLS = [
    {"name": "vertica_list_tables", "description": "List tables/views in Vertica.", "input_schema": {"type": "object", "properties": {"schema_pattern": {"type": "string"}, "table_pattern": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 500}}, "additionalProperties": False}},
    {"name": "vertica_describe_table", "description": "Describe a Vertica table.", "input_schema": {"type": "object", "properties": {"schema": {"type": "string"}, "table": {"type": "string"}}, "required": ["schema","table"], "additionalProperties": False}},
    {"name": "vertica_query_readonly", "description": "Execute read-only query with max rows.", "input_schema": {"type": "object", "properties": {"sql": {"type": "string"}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 200}}, "required": ["sql"], "additionalProperties": False}},
    {"name": "vertica_find", "description": "Search v_catalog.columns for keyword.", "input_schema": {"type": "object", "properties": {"keyword": {"type": "string"}, "schemas": {"type": "array", "items": {"type": "string"}}, "limit": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 200}}, "required": ["keyword"], "additionalProperties": False}},
    {"name": "vertica_probe_table", "description": "Sample a small number of rows from a table.", "input_schema": {"type": "object", "properties": {"schema": {"type": "string"}, "table": {"type": "string"}, "columns": {"type": "array", "items": {"type": "string"}}, "where": {"type": "string"}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 50, "default": 5}}, "required": ["schema","table"], "additionalProperties": False}},
    {"name": "vertica_export_query", "description": "Execute read-only SQL and export to CSV/JSON.", "input_schema": {"type": "object", "properties": {"sql": {"type": "string"}, "format": {"type": "string", "enum": ["csv","json"]}, "out_path": {"type": "string"}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 500000, "default": 200}, "label": {"type": "string"}, "approved": {"type": "boolean"}}, "required": ["sql","format","out_path"], "additionalProperties": False}},
    {"name": "vertica_run_sql_file", "description": "Read a .sql file from workspace and export results.", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}, "format": {"type": "string", "enum": ["csv","json"]}, "out_path": {"type": "string"}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 500000, "default": 200}, "label": {"type": "string"}, "approved": {"type": "boolean"}}, "required": ["path","format","out_path"], "additionalProperties": False}},
]

def _effective_schemas(per_call: List[str]) -> List[str]:
    per_call = [s.strip() for s in per_call if s and s.strip()]
    if SCHEMA_ALLOWLIST and per_call:
        return [s for s in per_call if s in SCHEMA_ALLOWLIST]
    if SCHEMA_ALLOWLIST:
        return SCHEMA_ALLOWLIST
    return per_call

def handle_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "vertica_list_tables":
        limit = int(args.get("limit", 500))
        schema_like = args.get("schema_pattern", "%")
        table_like = args.get("table_pattern", "%")

        sql = f"""SELECT table_schema, table_name, table_type
FROM v_catalog.tables
WHERE table_schema LIKE '{schema_like}'
  AND table_name   LIKE '{table_like}'
ORDER BY table_schema, table_name
LIMIT {limit}"""

        if SCHEMA_ALLOWLIST:
            allowed = ",".join([f"'{s}'" for s in SCHEMA_ALLOWLIST])
            sql = sql.replace("WHERE", f"WHERE table_schema IN ({allowed}) AND", 1)

        return jdbc_query(sql, max_rows=limit)

    if name == "vertica_describe_table":
        schema = args["schema"]
        table = args["table"]
        if SCHEMA_ALLOWLIST and schema not in SCHEMA_ALLOWLIST:
            raise ValueError(f"Schema '{schema}' is not in allowlist.")
        sql = """SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position"""
        return jdbc_query(sql, params=[schema, table], max_rows=2000)

    if name == "vertica_query_readonly":
        max_rows = int(args.get("max_rows", 200))
        sql_in = single_statement(str(args["sql"]))
        assert_readonly_sql(sql_in)
        sql2 = ensure_limit(sql_in, max_rows=max_rows)
        return jdbc_query(sql2, max_rows=max_rows)

    if name == "vertica_find":
        keyword = str(args["keyword"]).strip().lower()
        limit = int(args.get("limit", 200))
        schemas = _effective_schemas(args.get("schemas") or [])
        like = f"%{keyword}%"
        exact = keyword

        schema_filter = ""
        params: List[Any] = [exact, exact, like, like, like, like, like, like]
        if schemas:
            placeholders = ",".join(["?"] * len(schemas))
            schema_filter = f" AND c.table_schema IN ({placeholders}) "
            params += schemas

        sql = f"""SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.data_type,
  (
    CASE WHEN LOWER(c.table_name)  = ? THEN 100 ELSE 0 END +
    CASE WHEN LOWER(c.column_name) = ? THEN  90 ELSE 0 END +
    CASE WHEN LOWER(c.column_name) IN ('asset_id','assetid','asset_uuid','assetuuid','asset_tag','assettag') THEN 50 ELSE 0 END +
    CASE WHEN LOWER(c.column_name) LIKE ? THEN 40 ELSE 0 END +
    CASE WHEN LOWER(c.table_name)  LIKE ? THEN 30 ELSE 0 END
  ) AS score,
  CASE
    WHEN LOWER(c.table_name)  LIKE ? THEN 'table_name'
    WHEN LOWER(c.column_name) LIKE ? THEN 'column_name'
    ELSE 'other'
  END AS match_type
FROM v_catalog.columns c
WHERE (LOWER(c.table_name) LIKE ? OR LOWER(c.column_name) LIKE ?)
{schema_filter}
ORDER BY score DESC, c.table_schema, c.table_name, c.ordinal_position
LIMIT {limit}"""
        raw = jdbc_query(sql, params=params, max_rows=limit)
        cols = raw["columns"]
        records = [dict(zip(cols, row)) for row in raw["rows"]]
        return {**raw, "records": records, "applied_schema_filter": schemas}

    if name == "vertica_probe_table":
        schema = assert_safe_ident(args["schema"], "schema")
        table = assert_safe_ident(args["table"], "table")
        max_rows = max(1, min(int(args.get("max_rows", 5)), 50))
        if SCHEMA_ALLOWLIST and schema not in SCHEMA_ALLOWLIST:
            raise ValueError(f"Schema '{schema}' is not in allowlist.")
        cols = args.get("columns") or []
        cols = [assert_safe_ident(str(c), "column") for c in cols]
        where = args.get("where")
        if where is not None:
            where = str(where).strip()
            if where:
                assert_safe_where(where)
            else:
                where = None
        if not cols:
            desc_sql = """SELECT column_name FROM columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position
LIMIT 12"""
            desc = jdbc_query(desc_sql, params=[schema, table], max_rows=100)
            cols = [r[0] for r in desc["rows"]] or []
            if not cols:
                raise ValueError(f"Table not found or no columns: {schema}.{table}")
        select_list = ", ".join([f'"{c}"' for c in cols])
        sql = f'SELECT {select_list} FROM "{schema}"."{table}"'
        if where:
            sql += f" WHERE {where}"
        sql += f" LIMIT {max_rows}"
        return jdbc_query(sql, max_rows=max_rows)

    if name == "vertica_export_query":
        require_approval(args, "Exporting query results to disk")
        fmt = str(args["format"]).lower()
        out_path = resolve_under(EXPORT_ROOT, str(args["out_path"]))
        max_rows = max(1, min(int(args.get("max_rows", 200)), 500000))
        label = str(args.get("label") or "").strip()

        sql_in = single_statement(str(args["sql"]))
        assert_readonly_sql(sql_in)
        sql2 = ensure_limit(sql_in, max_rows=max_rows)

        h = sql_hash(sql2)
        t0 = time.time()
        result = jdbc_query(sql2, max_rows=max_rows)
        ms = int((time.time() - t0) * 1000)

        meta = {
            "generated_at": utc_now_iso(),
            "source": "vertica",
            "tool": "vertica_export_query",
            "label": label or None,
            "sql_hash": h,
            "max_rows": max_rows,
            "row_count": result.get("row_count"),
            "truncated": result.get("truncated"),
            "columns": result.get("columns"),
        }

        export_payload = {**result, "records": to_records(result["columns"], result["rows"])}
        write_export(export_payload, fmt, out_path, meta)

        audit_log({
            "status": "ok",
            "tool": "vertica_export_query",
            "sql_hash": h,
            "format": fmt,
            "out_path": str(out_path),
            "max_rows": max_rows,
            "row_count": result.get("row_count"),
            "truncated": result.get("truncated"),
            "execution_ms": ms,
        })

        return {**result, "meta": {"tool": "vertica_export_query", "execution_ms": ms, "sql_hash": h, "saved_path": str(out_path), "label": label or None}}

    if name == "vertica_run_sql_file":
        require_approval(args, "Running a SQL file and exporting results to disk")
        fmt = str(args["format"]).lower()
        out_path = resolve_under(EXPORT_ROOT, str(args["out_path"]))
        max_rows = max(1, min(int(args.get("max_rows", 200)), 500000))
        label = str(args.get("label") or "").strip()

        sql_path = resolve_under(WORKSPACE_ROOT, str(args["path"]))
        sql_text = read_sql_file(sql_path)
        sql_text = single_statement(sql_text)
        assert_readonly_sql(sql_text)
        sql2 = ensure_limit(sql_text, max_rows=max_rows)

        h = sql_hash(sql2)
        t0 = time.time()
        result = jdbc_query(sql2, max_rows=max_rows)
        ms = int((time.time() - t0) * 1000)

        meta = {
            "generated_at": utc_now_iso(),
            "source": "vertica",
            "tool": "vertica_run_sql_file",
            "label": label or sql_path.name,
            "sql_hash": h,
            "sql_file": str(sql_path),
            "max_rows": max_rows,
            "row_count": result.get("row_count"),
            "truncated": result.get("truncated"),
            "columns": result.get("columns"),
        }

        export_payload = {**result, "records": to_records(result["columns"], result["rows"])}
        write_export(export_payload, fmt, out_path, meta)

        audit_log({
            "status": "ok",
            "tool": "vertica_run_sql_file",
            "sql_hash": h,
            "sql_file": str(sql_path),
            "format": fmt,
            "out_path": str(out_path),
            "max_rows": max_rows,
            "row_count": result.get("row_count"),
            "truncated": result.get("truncated"),
            "execution_ms": ms,
        })

        return {**result, "meta": {"tool": "vertica_run_sql_file", "execution_ms": ms, "sql_hash": h, "saved_path": str(out_path), "sql_file": str(sql_path), "label": label or sql_path.name}}

    raise ValueError(f"Unknown tool: {name}")

def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            method = msg.get("method")

            if method == "tools/list":
                sys.stdout.write(json.dumps({"tools": TOOLS}) + "\n")
                sys.stdout.flush()
                continue

            if method == "tools/call":
                params = msg.get("params", {})
                tool_name = params.get("name")
                tool_args = params.get("arguments", {}) or {}

                t0 = time.time()
                try:
                    result = handle_call(tool_name, tool_args)
                    total_ms = int((time.time() - t0) * 1000)
                    if isinstance(result, dict):
                        result.setdefault("meta", {})
                        if isinstance(result["meta"], dict):
                            result["meta"].setdefault("total_ms", total_ms)
                    sys.stdout.write(json.dumps({"result": result}, default=str) + "\n")
                    sys.stdout.flush()
                except Exception as e:
                    audit_log({"status": "error", "tool": tool_name, "error": str(e)})
                    sys.stdout.write(json.dumps({"error": str(e)}) + "\n")
                    sys.stdout.flush()
                continue

            sys.stdout.write(json.dumps({"error": f"Unknown method: {method}"}) + "\n")
            sys.stdout.flush()

        except Exception as e:
            sys.stdout.write(json.dumps({"error": str(e)}) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
