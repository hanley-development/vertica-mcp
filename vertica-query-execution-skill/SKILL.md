---
name: vertica-query-execution
description: Execute Vertica queries or .sql files via MCP, export results to CSV/JSON, and interpret or format results safely.
---

# Vertica Query Execution Skill

## Purpose
Generic Vertica execution workflow using MCP tools:
- Run ad-hoc SQL safely
- Run .sql files
- Export to CSV or JSON
- Interpret and summarize results

## Expected MCP Tools
- vertica_query_readonly(sql, max_rows)
- vertica_export_query(sql, format, out_path, max_rows)
- vertica_run_sql_file(path, format, out_path, max_rows)
- vertica_list_tables
- vertica_describe_table
- vertica_find
- vertica_probe_table

Prefer export tools when available.

## Safety Defaults
- SELECT / WITH only unless confirmed otherwise
- No multi-statement execution
- Enforce row limits
- Avoid SELECT *
- Show <=10 rows in chat unless requested

Default max_rows:
- discovery: 200
- probe: 5 (max 50)
- analysis/export: 200 unless overridden

## Standard Workflows

### Run Ad-Hoc Query
1. Validate read-only intent
2. Apply max_rows if missing
3. If export requested:
   - Use vertica_export_query(sql, format, out_path, max_rows)
4. Otherwise:
   - Use vertica_query_readonly(sql, max_rows)
5. Summarize row_count + truncated

### Run .sql File
1. Validate file exists
2. Prefer vertica_run_sql_file(path, format, out_path, max_rows)
3. If export requested:
   - Save under exports/vertica/<timestamp>_<label>.<ext>

### Interpretation Rules
- Always include row_count + truncated
- Show small sample only
- Provide aggregates when helpful
- Clearly state join keys when merging

## Output Contract
{
  columns: [...],
  rows: [...],
  row_count: N,
  truncated: true|false,
  meta: {
    tool: "...",
    execution_ms: ...,
    max_rows: ...,
    source: "vertica"
  }
}
