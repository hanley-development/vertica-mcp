# Cline Project Starter
## Project: MCP Vertica Skill (Read-Only)

---

# 0) Project Contract

**PROJECT_NAME:** mcp-vertica  
**OWNER:** Michael Hanley  
**PRIMARY_USER:** Cline (VS Code)  
**PRIMARY_INTERFACE:** MCP over stdio  
**RUNTIME:** Python (containerized)  

---

## Objective

Provide Cline with a safe, deterministic, read-only Vertica “skill” via an MCP stdio server.

The server must support:
- Schema discovery
- Controlled table exploration
- Bounded SELECT queries
- Strict read-only enforcement

---

## Why This Exists

Cline requires structured, deterministic database access to:
- Avoid hallucinating schema
- Explore tables safely
- Execute bounded analytical queries
- Maintain enterprise-grade safety controls

This project creates a controlled database skill layer between Cline and Vertica.

---

## Success Criteria (Measurable)

- [ ] Cline can successfully call `health_check`
- [ ] Cline can list tables via `vertica_list_tables`
- [ ] Cline can inspect columns via `vertica_describe_table`
- [ ] Cline can sample data via `vertica_probe_table`
- [ ] Cline can execute bounded read-only SELECT queries
- [ ] Mutating SQL is rejected 100% of the time
- [ ] Multi-statement queries are rejected
- [ ] Hard row cap enforced
- [ ] Query timeout enforced
- [ ] Secrets never appear in logs
- [ ] Tool outputs are structured JSON

---

## Non-Goals (v0.1)

- No INSERT / UPDATE / DELETE
- No DDL (CREATE / DROP / ALTER)
- No async job queue
- No UI
- No write capability
- No cross-database abstraction layer

---

## Constraints

- Python 3.11+
- Containerized execution
- MCP over stdio (required)
- Vertica via ODBC (preferred)
- Config via environment variables
- Strict read-only enforcement at server level

---

## Security Requirements (Non-Negotiable)

- Enforce read-only at SQL validation layer
- Reject:
  - INSERT
  - UPDATE
  - DELETE
  - MERGE
  - CREATE
  - DROP
  - ALTER
  - COPY
  - CALL
- Reject semicolons / multi-statements
- Enforce LIMIT
- Hard max rows (default 200, max 500)
- Query timeout (default 15 seconds)
- Redact credentials in logs

---

# 1) Interaction Rules for Cline

Cline operates in **Structured Engineering Mode**.

Rules:

- Design first.
- No code until tool contracts are defined.
- No silent assumptions.
- All behavior must be deterministic.
- All tools must return structured JSON.
- Errors must be structured objects (not strings).
- Prefer explicit enforcement over convention.
- All major tasks must apply Development Memory Rule.
- All tasks must apply Task Completion Rule.

---

# 2) Tool Surface (v0.1)

Required Tools:

1. `health_check`
2. `vertica_list_tables`
3. `vertica_describe_table`
4. `vertica_probe_table`
5. `vertica_query_readonly`

All tools must define:
- Input schema
- Output schema
- Error codes
- Guardrails
- Examples

No implicit behavior.

---

# 3) Architecture Model

Cline
↓
MCP stdio server
↓
Tool Router
↓
Policy Engine (SQL Guard)
↓
Vertica Adapter (ODBC)
↓
Vertica

Separation of concerns:

- server.py → MCP wiring only
- tools/ → tool logic only
- policy/ → enforcement only
- adapters/ → DB communication only
- logging/ → structured logs only

No mixing responsibilities.

---

# 4) Policy Model

Read-only enforcement is performed via:

- Keyword blocking
- Statement structure validation
- LIMIT injection
- Hard row cap enforcement
- Timeout enforcement

Policy must operate before DB execution.

---

# 5) Observability Contract

Each request must log:

- timestamp
- trace_id
- tool_name
- execution_time_ms
- status (ok/error)
- error_code (if applicable)

Secrets must be redacted.

---

# 6) Development Memory Rule (Required)

When a task is completed, determine if it introduced:

- An architectural decision
- A coding pattern
- A constraint
- A workaround
- A gotcha

If yes, update:

docs/dev-memory/
- ARCHITECTURE.md
- DECISIONS.md
- PATTERNS.md
- GOTCHAS.md

Use concise bullets.
Include WHY.
Do not restate obvious code.

If none apply, explicitly state:

"No persistent development memory update required."

---

# 7) Task Completion Rule (Required)

At the end of every task:

- Provide concise summary (high-signal bullets)
- Apply Development Memory Rule
- Clearly state one of:

"Memory updated"
OR
"No persistent development memory update required"

---

# 8) Definition of Done (Project-Level)

- Repo scaffold complete
- Tool contracts documented
- Policy engine implemented
- MCP server runs in container
- All v0.1 tools implemented
- Tests for SQL guard
- RUNBOOK written
- Tool catalog documented
- Example Cline invocation included

---

# 9) Kickoff Prompt (For Cline)

We are building an MCP server that provides a read-only Vertica skill.

OBJECTIVE:
Allow Cline to safely explore schema and execute bounded SELECT queries.

CONSTRAINTS:
- MCP over stdio
- Python container
- Read-only enforcement required
- No multi-statement SQL
- Hard row limit
- Query timeout
- Structured JSON outputs

MODE:
Design first. No code until architecture and tool contracts are agreed.

Development Memory Rule applies.
Task Completion Rule applies.

---
