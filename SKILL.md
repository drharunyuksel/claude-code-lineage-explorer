---
name: data-lineage
description: "Build and query a data lineage graph from your codebase and BigQuery metadata. Use this skill for ANY question about where data comes from, where it goes, or how tables relate to each other. Example questions: 'What are the source tables for the users table?', 'Which pipeline writes to orders?', 'What feeds into the revenue dashboard?', 'Where does this table get its data from?', 'What tables does the daily_sync DAG write to?', 'Show me upstream dependencies for this view', 'What would break if I change this table?'. Triggers on: /data-lineage, source tables, feeds into, depends on, writes to, upstream, downstream, data flow, lineage, table dependencies, where does this table come from, which pipeline, impact analysis."
---

# BigQuery Lineage Builder

Build a complete table lineage graph from your codebase and BigQuery metadata. Output is a SQLite database (`lineage.db`) in the skill directory.

---

## Installation

This skill must be installed inside the project's `.claude/skills/` directory, **not** the global `~/.claude/skills/`. This ensures it is scoped to the repository.

```
<project-root>/.claude/skills/data-lineage/
├── SKILL.md
├── scripts/
│   ├── schema.sql
│   ├── insert_job_history.py
│   ├── airflow_scan.py
│   ├── generic_scan.py
│   └── parse_views.py
├── lineage.db      (created automatically on first run)
└── .venv/          (created automatically on first run)
```

Add to `.gitignore`:

```
.claude/skills/data-lineage/.venv/
.claude/skills/data-lineage/lineage.db
```

The venv and database are created automatically and rebuilt on demand — neither should be committed.

---

## Step 0: Staleness Check

Before doing anything else, check if `lineage.db` already exists in the skill directory (`SKILL_DIR`, resolved in Step 1b).

**If it exists**, check its age via Bash:

```bash
SKILL_DIR=".claude/skills/data-lineage"
DB_PATH="$SKILL_DIR/lineage.db"
if [ -f "$DB_PATH" ]; then
  AGE_DAYS=$(( ($(date +%s) - $(stat -f %m "$DB_PATH" 2>/dev/null || stat -c %Y "$DB_PATH")) / 86400 ))
  echo "$AGE_DAYS"
fi
```

Then decide:

- **If the user asked a lineage question** (e.g., "which pipeline writes to X?", "show me source tables for Y") **and the database is less than 7 days old**: Tell the user: "Lineage database is current (last updated {AGE_DAYS} day(s) ago). Querying..." Then skip the build and go directly to querying `lineage.db` with SQLite to answer the question. Do NOT rebuild.
- **If the database is 7 or more days old**: Tell the user: "The lineage database is {AGE_DAYS} days old. Would you like me to refresh it before I answer?" If yes, proceed to Step 1. If no, query the existing database.
- **If the user explicitly asked to build/rebuild lineage** (e.g., `/data-lineage`, "rebuild lineage", "refresh lineage"): Always proceed to Step 1 regardless of age.

**If it does not exist**, proceed to Step 1.

The staleness threshold is 7 days.

---

## Step 1: Prerequisites Check

### 1a. Verify BigQuery MCP tool

Run this query using `mcp__bigquery_local__query`:

```sql
SELECT 1 AS ok
```

If the tool is not available or the query fails, stop and tell the user:

> "The BigQuery MCP tool (`mcp__bigquery_local__query`) is required but not configured. Please add it to your MCP settings. See: https://github.com/ergut/mcp-bigquery-server"

### 1b. Set up Python virtual environment

The skill directory is `.claude/skills/data-lineage/` inside the current project (the directory containing this SKILL.md file). Set `SKILL_DIR` to that path. Then run via Bash:

```bash
SKILL_DIR="<resolved path to data-lineage directory>"
if [ ! -d "$SKILL_DIR/.venv" ]; then
  python3 -m venv "$SKILL_DIR/.venv"
  "$SKILL_DIR/.venv/bin/pip" install --quiet sqlglot
  echo "Created venv and installed sqlglot"
else
  echo "Venv already exists"
fi
VENV_PYTHON="$SKILL_DIR/.venv/bin/python3"
```

Store `VENV_PYTHON` for use in later steps. If venv creation fails, note that view definition parsing will be skipped.

---

## Step 2: Auto-detect project_id

Use Grep to find the first BigQuery fully-qualified table reference in the codebase:

```
pattern: [a-z][a-z0-9-]+\.[a-zA-Z_]\w+\.[a-zA-Z_]\w+
```

Extract the first segment (before the first dot) as `project_id`. Confirm with the user: "Detected project ID: `{project_id}`. Is this correct?"

Set `DB_PATH` to `$SKILL_DIR/lineage.db` and `LOOKBACK_DAYS` to `60`.

---

## Step 3: Orchestrator Detection

Detect whether this is an Airflow repository:

1. Use Grep to search for `from airflow` or `import airflow` in `*.py` files
2. Use Glob to check for a `dags/` directory at the repo root
3. Use Grep to search for `airflow` in `requirements.txt` or `pyproject.toml`

If any of these match: `ORCHESTRATOR=airflow`. Otherwise: `ORCHESTRATOR=generic`.

For Airflow, also auto-detect `dags_dir` if not configured: use the `dags/` directory if it exists, otherwise the repo root.

Print: `"Detected orchestrator: {ORCHESTRATOR}"`

---

## Step 4: Create SQLite Database

Run via Bash:

```bash
DB_PATH="$SKILL_DIR/lineage.db"
rm -f "$DB_PATH"
sqlite3 "$DB_PATH" < "$SKILL_DIR/scripts/schema.sql"
echo "Created $DB_PATH"
```

---

## Step 5: BQ Job History (edge_source = 'job_history')

### 5a. Query INFORMATION_SCHEMA.JOBS

Run this query using `mcp__bigquery_local__query`. Replace `{project_id}`, `{location}`, and `{lookback_days}` with resolved values:

```sql
WITH job_edges AS (
  SELECT
    job_id,
    statement_type,
    destination_table,
    referenced_tables,
    labels,
    user_email,
    creation_time
  FROM `region-{location}`.INFORMATION_SCHEMA.JOBS
  WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    AND state = 'DONE'
    AND error_result IS NULL
    AND statement_type IS NOT NULL
    AND statement_type NOT IN ('SELECT', 'SCRIPT')
    AND destination_table.dataset_id NOT LIKE r'\_%'
),
flat_edges AS (
  SELECT
    CONCAT(ref.dataset_id, '.', ref.table_id) AS source_fq,
    destination_table.dataset_id AS target_dataset,
    destination_table.table_id AS target_name,
    (SELECT l.value FROM UNNEST(labels) AS l WHERE l.key = 'airflow-dag') AS dag_label,
    statement_type,
    user_email,
    creation_time,
    job_id
  FROM job_edges
  CROSS JOIN UNNEST(referenced_tables) AS ref
  WHERE ref.dataset_id NOT LIKE r'\_%'
    AND NOT (
      ref.project_id = destination_table.project_id
      AND ref.dataset_id = destination_table.dataset_id
      AND ref.table_id = destination_table.table_id
    )
)
SELECT
  target_dataset,
  target_name,
  'TABLE' AS target_type,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT source_fq ORDER BY source_fq), ',') AS source_tables_csv,
  CASE
    WHEN ANY_VALUE(dag_label) IS NOT NULL THEN ANY_VALUE(dag_label)
    ELSE ANY_VALUE(user_email)
  END AS pipeline_id,
  CASE
    WHEN ANY_VALUE(dag_label) IS NOT NULL THEN 'airflow_dag'
    ELSE 'script'
  END AS pipeline_type,
  'job_history' AS edge_source,
  MAX_BY(statement_type, creation_time) AS write_pattern,
  CAST(NULL AS STRING) AS view_definition,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MIN(creation_time)) AS first_seen,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(creation_time)) AS last_seen,
  COUNT(DISTINCT job_id) AS job_count,
  ANY_VALUE(user_email) AS user_email
FROM flat_edges
GROUP BY target_dataset, target_name
```

### 5b. Insert results into SQLite

The MCP tool returns a JSON array. Save the result as a JSON string and pipe it into the insert script:

```bash
echo '<JSON_RESULT>' | $VENV_PYTHON "$SKILL_DIR/scripts/insert_job_history.py" "$DB_PATH"
```

Replace `<JSON_RESULT>` with the actual JSON output from the MCP query. If the result is too large, write it to a temp file first and read from there.

---

## Step 6: Codebase Scan (edge_source = 'codebase_scan')

### If ORCHESTRATOR = airflow

```bash
$VENV_PYTHON "$SKILL_DIR/scripts/airflow_scan.py" \
  --dags-dir "<DAGS_DIR>" \
  --project-id "<PROJECT_ID>" \
  --db-path "$DB_PATH"
```

Replace `<DAGS_DIR>` and `<PROJECT_ID>` with the resolved configuration values.

### If ORCHESTRATOR = generic

```bash
$VENV_PYTHON "$SKILL_DIR/scripts/generic_scan.py" \
  --repo-dir "<REPO_ROOT>" \
  --project-id "<PROJECT_ID>" \
  --db-path "$DB_PATH"
```

---

## Step 7: View Definition Parsing (edge_source = 'view_definition')

Skip this step if the venv setup failed in Step 1b.

### 7a. Query view definitions

Run this query using `mcp__bigquery_local__query`. Replace `{location}` with the configured location:

```sql
SELECT
  table_schema AS dataset,
  table_name AS view_name,
  view_definition
FROM `region-{location}`.INFORMATION_SCHEMA.VIEWS
WHERE table_schema NOT LIKE r'\_%'
  AND table_schema != 'INFORMATION_SCHEMA'
```

### 7b. Parse with sqlglot and insert into SQLite

Save the MCP query result as JSON to a temp file, then run the parser:

```bash
echo '<VIEWS_JSON>' > /tmp/lineage_views.json
$VENV_PYTHON "$SKILL_DIR/scripts/parse_views.py" "$DB_PATH" /tmp/lineage_views.json
```

Replace `<VIEWS_JSON>` with the JSON output from the MCP query in Step 7a. For large result sets, write the JSON to the temp file using a heredoc or Python.

---

## Step 8: Summary

Run these queries via Bash and display the results:

```bash
DB_PATH="$SKILL_DIR/lineage.db"

TABLES=$(sqlite3 "$DB_PATH" "SELECT COUNT(DISTINCT target_dataset || '.' || target_name) FROM lineage WHERE target_type='TABLE'")
VIEWS=$(sqlite3 "$DB_PATH" "SELECT COUNT(DISTINCT target_dataset || '.' || target_name) FROM lineage WHERE target_type='VIEW'")
PIPELINES=$(sqlite3 "$DB_PATH" "SELECT COUNT(DISTINCT pipeline_id) FROM lineage WHERE pipeline_id IS NOT NULL")
DATASETS=$(sqlite3 "$DB_PATH" "SELECT COUNT(DISTINCT target_dataset) FROM lineage")
JOB_HIST=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM lineage WHERE edge_source='job_history'")
CODE_SCAN=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM lineage WHERE edge_source='codebase_scan'")
VIEW_DEF=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM lineage WHERE edge_source='view_definition'")
```

Then print (output this text directly to the user):

```
Lineage scan complete.

  Orchestrator:  {ORCHESTRATOR} ({PIPELINES} pipelines detected)
  Tables:        {TABLES}
  Views:         {VIEWS}
  Pipelines:     {PIPELINES}
  Datasets:      {DATASETS}
  Sources:       job_history ({JOB_HIST}), codebase_scan ({CODE_SCAN}), view_definition ({VIEW_DEF})

  Database:      ./{DB_PATH}

Example queries:
  sqlite3 {DB_PATH} "SELECT pipeline_id, edge_source FROM lineage WHERE target_name = '<table>'"
  sqlite3 {DB_PATH} "SELECT source_tables FROM lineage WHERE target_name = '<table>'"
  sqlite3 {DB_PATH} "SELECT * FROM table_summary ORDER BY target_dataset, target_name"
```

---

## Step 9: Cleanup

Remove temp files:

```bash
rm -f /tmp/lineage_views.json
```
