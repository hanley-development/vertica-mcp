---
name: vertica-schema-discovery-modeling
description: Use when exploring Vertica schemas/tables, documenting data models, identifying keys/relationships, and recommending star-schema or join patterns.
---

# Vertica Schema Discovery & Data Modeling Skill

## Purpose
Help discover existing Vertica structures and produce usable “data model” outputs:
- list schemas/tables/views
- find likely keys and relationships
- identify dimensions vs facts
- document entities, attributes, and join paths
- propose modeling patterns (star/snowflake) for analytics or reporting

## Expected MCP Tools
Prefer:
- vertica_list_tables(schema_pattern?, table_pattern?, limit?)
- vertica_describe_table(schema, table)
- vertica_find(keyword, schemas?, limit?)
- vertica_probe_table(schema, table, columns?, where?, max_rows?)
- vertica_query_readonly(sql, max_rows?)

## Workflow: Discover → Characterize → Relate → Model

### 1) Discover
- Start with vertica_list_tables using schema/table patterns.
- Use vertica_find for domain keywords (asset, incident, capa, patient, order, etc.).

### 2) Characterize tables
For each candidate table:
- Use vertica_describe_table to collect columns + types.
- Identify:
  - primary-key candidates (id, *_id, uuid, natural keys)
  - timestamp columns (created_at, updated_at, event_ts)
  - status columns (state, status, type)
- Use vertica_probe_table (max_rows=5) only if needed to confirm format.

### 3) Infer relationships
Infer likely joins by:
- shared key names (asset_id in multiple tables)
- consistent data types and cardinality hints
- referential patterns (dim_* vs fact_*)

Validate with small, safe queries:
- COUNT(DISTINCT key)
- NULL rate checks
- join match rate tests (LEFT JOIN ... WHERE right.key IS NULL)

### 4) Produce a model
Output one or more artifacts:
- Entity list: table, purpose, grain, key columns
- Relationship map: join keys and join direction
- Suggested star schema:
  - Fact table(s): grain, measures, foreign keys
  - Dimension tables: attributes, slowly changing suggestions
- “Query path recipes”: common joins for reporting

## Deliverables (preferred formats)
- Markdown model doc (tables + bullets)
- Mermaid ER diagram (when helpful)
- YAML/JSON model definition (if user asks)

## Guardrails
- Avoid dumping raw sensitive data; prefer structural info and aggregates.
- Keep probes tiny (<=5 rows).
- Always state assumptions and confidence for inferred relationships.
