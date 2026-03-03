import json
import os
import re
import sys
import time
import jpype
import jpype.imports

JDBC_JAR = os.environ.get("VERTICA_JDBC_JAR", "/app/drivers/vertica-jdbc.jar")
HOST = os.environ.get("VERTICA_HOST")
PORT = os.environ.get("VERTICA_PORT", "5433")
DB = os.environ.get("VERTICA_DB")
USER = os.environ.get("VERTICA_USER")
PASS = os.environ.get("VERTICA_PASS")

SCHEMA_ALLOWLIST = [
    s.strip() for s in (os.environ.get("VERTICA_SCHEMA_ALLOWLIST", "")).split(",") if s.strip()
]

if not all([HOST, DB, USER, PASS]):
    raise SystemExit("Missing required VERTICA_* environment variables.")

JDBC_URL = f"jdbc:vertica://{HOST}:{PORT}/{DB}"

BLOCKED = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|COPY|GRANT|REVOKE)\b", re.I)
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def start_jvm():
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JDBC_JAR])

def assert_readonly_sql(sql):
    if ";" in sql:
        raise ValueError("Multi-statement SQL is not allowed.")
    if not sql.strip().upper().startswith(("SELECT", "WITH")):
        raise ValueError("Only SELECT/WITH allowed.")
    if BLOCKED.search(sql):
        raise ValueError("Blocked DDL/DML keyword detected.")

def assert_safe_ident(name):
    if not IDENT.match(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

def jdbc_query(sql, params=None, max_rows=200):
    start_jvm()
    from java.sql import DriverManager
    conn = DriverManager.getConnection(JDBC_URL, USER, PASS)
    try:
        if params:
            stmt = conn.prepareStatement(sql)
            for i, p in enumerate(params, 1):
                stmt.setObject(i, p)
            stmt.setMaxRows(max_rows)
            rs = stmt.executeQuery()
        else:
            stmt = conn.createStatement()
            stmt.setMaxRows(max_rows)
            rs = stmt.executeQuery(sql)

        md = rs.getMetaData()
        cols = [md.getColumnLabel(i) for i in range(1, md.getColumnCount()+1)]
        rows = []
        while rs.next() and len(rows) < max_rows:
            rows.append([rs.getObject(i) for i in range(1, md.getColumnCount()+1)])
        return {"columns": cols, "rows": rows, "row_count": len(rows), "truncated": len(rows) >= max_rows}
    finally:
        conn.close()

TOOLS = [
    {"name": "vertica_list_tables", "input_schema": {"type": "object", "properties": {"schema_pattern": {"type": "string"}, "table_pattern": {"type": "string"}, "limit": {"type": "integer"}}, "additionalProperties": False}},
    {"name": "vertica_describe_table", "input_schema": {"type": "object", "properties": {"schema": {"type": "string"}, "table": {"type": "string"}}, "required": ["schema","table"], "additionalProperties": False}},
    {"name": "vertica_query_readonly", "input_schema": {"type": "object", "properties": {"sql": {"type": "string"}, "max_rows": {"type": "integer"}}, "required": ["sql"], "additionalProperties": False}},
    {"name": "vertica_find", "input_schema": {"type": "object", "properties": {"keyword": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["keyword"], "additionalProperties": False}},
    {"name": "vertica_probe_table", "input_schema": {"type": "object", "properties": {"schema": {"type": "string"}, "table": {"type": "string"}, "columns": {"type": "array","items":{"type":"string"}}, "max_rows": {"type": "integer"}}, "required": ["schema","table"], "additionalProperties": False}}
]

def handle_call(name, args):
    if name == "vertica_list_tables":
        sql = "SELECT table_schema, table_name FROM v_catalog.tables LIMIT 500"
        return jdbc_query(sql)
    if name == "vertica_describe_table":
        sql = "SELECT column_name, data_type FROM columns WHERE table_schema=? AND table_name=?"
        return jdbc_query(sql, [args["schema"], args["table"]])
    if name == "vertica_query_readonly":
        assert_readonly_sql(args["sql"])
        return jdbc_query(args["sql"], max_rows=args.get("max_rows",200))
    if name == "vertica_find":
        like = f"%{args['keyword'].lower()}%"
        sql = "SELECT table_schema, table_name, column_name FROM v_catalog.columns WHERE LOWER(column_name) LIKE ? OR LOWER(table_name) LIKE ? LIMIT 200"
        return jdbc_query(sql, [like, like])
    if name == "vertica_probe_table":
        schema = assert_safe_ident(args["schema"])
        table = assert_safe_ident(args["table"])
        cols = args.get("columns") or ["*"]
        col_sql = ", ".join(cols)
        sql = f'SELECT {col_sql} FROM "{schema}"."{table}" LIMIT {args.get("max_rows",5)}'
        return jdbc_query(sql)
    raise ValueError("Unknown tool")

def main():
    for line in sys.stdin:
        msg = json.loads(line.strip())
        if msg.get("method") == "tools/list":
            print(json.dumps({"tools": TOOLS}))
        elif msg.get("method") == "tools/call":
            name = msg["params"]["name"]
            args = msg["params"].get("arguments", {})
            result = handle_call(name, args)
            print(json.dumps({"result": result}, default=str))
        else:
            print(json.dumps({"error": "Unknown method"}))

if __name__ == "__main__":
    main()
