---
name: lineage-explorer
description: "Scan a local codebase and query BigQuery INFORMATION_SCHEMA to build a table lineage graph stored in SQLite. Use when asked to build lineage, map pipelines to tables, answer 'which pipeline writes to this table?', or trace BigQuery table dependencies. Triggers on: /lineage-explorer, 'build lineage', 'table lineage', 'pipeline mapping'."
---

# BigQuery Lineage Builder

Build a complete table lineage graph from your codebase and BigQuery metadata. Output is a SQLite database (`lineage.db`) in the skill directory.

---

## Installation

This skill must be installed inside the project's `.claude/skills/` directory, **not** the global `~/.claude/skills/`. This ensures it is scoped to the repository.

```
<project-root>/.claude/skills/lineage-explorer/
├── SKILL.md
├── lineage.db      (created automatically on first run)
└── .venv/          (created automatically on first run)
```

Add to `.gitignore`:

```
.claude/skills/lineage-explorer/.venv/
.claude/skills/lineage-explorer/lineage.db
```

The venv and database are created automatically and rebuilt on demand — neither should be committed.

---

## Step 0: Staleness Check

Before doing anything else, check if `lineage.db` already exists in the skill directory (`SKILL_DIR`, resolved in Step 1b).

**If it exists**, check its age via Bash:

```bash
SKILL_DIR=".claude/skills/lineage-explorer"
DB_PATH="$SKILL_DIR/lineage.db"
if [ -f "$DB_PATH" ]; then
  AGE_DAYS=$(( ($(date +%s) - $(stat -f %m "$DB_PATH" 2>/dev/null || stat -c %Y "$DB_PATH")) / 86400 ))
  echo "$AGE_DAYS"
fi
```

Then decide:

- **If the user asked a lineage question** (e.g., "which pipeline writes to X?", "show me source tables for Y") **and the database is less than 7 days old**: Tell the user: "Lineage database is current (last updated {AGE_DAYS} day(s) ago). Querying..." Then skip the build and go directly to querying `lineage.db` with SQLite to answer the question. Do NOT rebuild.
- **If the database is 7 or more days old**: Tell the user: "The lineage database is {AGE_DAYS} days old. Would you like me to refresh it before I answer?" If yes, proceed to Step 1. If no, query the existing database.
- **If the user explicitly asked to build/rebuild lineage** (e.g., `/lineage-explorer`, "rebuild lineage", "refresh lineage"): Always proceed to Step 1 regardless of age.

**If it does not exist**, proceed to Step 1.

The staleness threshold (default: 7 days) can be overridden in `.lineage-explorer.json`:

```json
{
  "refresh_after_days": 7
}
```

---

## Step 1: Prerequisites Check

### 1a. Verify BigQuery MCP tool

Run this query using `mcp__bigquery_local__query`:

```sql
SELECT 1 AS ok
```

If the tool is not available or the query fails, stop and tell the user:

> "The BigQuery MCP tool (`mcp__bigquery_local__query`) is required but not configured. Please add it to your MCP settings. See: https://github.com/ergut/mcp-bigquery"

### 1b. Set up Python virtual environment

The skill directory is `.claude/skills/lineage-explorer/` inside the current project (the directory containing this SKILL.md file). Set `SKILL_DIR` to that path. Then run via Bash:

```bash
SKILL_DIR="<resolved path to lineage-explorer directory>"
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

## Step 2: Configuration

### 2a. Check for config file

Use the Read tool to check if `.lineage-explorer.json` exists in the repo root. If it does, parse it for these fields:

| Field | Default | Description |
|---|---|---|
| `project_id` | auto-detect | GCP project ID |
| `location` | `US` | BigQuery location |
| `lookback_days` | `60` | How far back to query job history (max ~60 days via MCP tool billing limit) |
| `dags_dir` | auto-detect | Directory containing DAG files |
| `exclude_datasets` | `[]` | Datasets to exclude from results |
| `db_path` | `<SKILL_DIR>/lineage.db` | Output SQLite path |

### 2b. Auto-detect project_id

If `project_id` is not set, use Grep to find the first BigQuery fully-qualified table reference in the codebase:

```
pattern: [a-z][a-z0-9-]+\.[a-zA-Z_]\w+\.[a-zA-Z_]\w+
```

Extract the first segment (before the first dot) as `project_id`. Confirm with the user: "Detected project ID: `{project_id}`. Is this correct?"

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
DB_PATH="<configured db_path, default: lineage.db>"
rm -f "$DB_PATH"
sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE lineage (
  target_dataset  TEXT    NOT NULL,
  target_name     TEXT    NOT NULL,
  target_type     TEXT    NOT NULL,
  source_tables   TEXT,
  pipeline_id     TEXT    NOT NULL DEFAULT '',
  pipeline_type   TEXT,
  edge_source     TEXT    NOT NULL,
  write_pattern   TEXT,
  view_definition TEXT,
  first_seen      TEXT,
  last_seen       TEXT,
  job_count       INTEGER,
  user_email      TEXT,
  PRIMARY KEY (target_dataset, target_name, pipeline_id, edge_source)
);

CREATE VIEW IF NOT EXISTS table_summary AS
SELECT
  target_dataset,
  target_name,
  target_type,
  GROUP_CONCAT(DISTINCT pipeline_id) AS pipelines,
  GROUP_CONCAT(DISTINCT edge_source) AS sources,
  MAX(last_seen) AS last_written,
  SUM(job_count) AS total_jobs
FROM lineage
GROUP BY target_dataset, target_name, target_type;
SQL
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

The MCP tool returns a JSON array. Write a Python script via Bash to parse the result and insert into SQLite.

Save the MCP query result as a JSON string. Then run via Bash:

```bash
$VENV_PYTHON -c "
import json, sqlite3, sys

data = json.loads(sys.stdin.read())
conn = sqlite3.connect('$DB_PATH')
cur = conn.cursor()

for row in data:
    source_csv = row.get('source_tables_csv', '')
    source_json = json.dumps(source_csv.split(',')) if source_csv else '[]'
    cur.execute('''
        INSERT OR REPLACE INTO lineage
        (target_dataset, target_name, target_type, source_tables, pipeline_id,
         pipeline_type, edge_source, write_pattern, view_definition,
         first_seen, last_seen, job_count, user_email)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        row['target_dataset'], row['target_name'], row['target_type'],
        source_json, row.get('pipeline_id'), row.get('pipeline_type'),
        row['edge_source'], row.get('write_pattern'),
        row.get('view_definition'),
        row.get('first_seen'), row.get('last_seen'),
        row.get('job_count', 0), row.get('user_email')
    ))

conn.commit()
print(f'Inserted {len(data)} job_history rows')
conn.close()
" <<< '<JSON_RESULT>'
```

Replace `<JSON_RESULT>` with the actual JSON output from the MCP query. If the result is too large, write it to a temp file first and read from there.

---

## Step 6: Codebase Scan (edge_source = 'codebase_scan')

### If ORCHESTRATOR = airflow

Write the following Python script to a temp file and execute it via `$VENV_PYTHON`. This script is adapted from a production-tested Airflow lineage builder.

```bash
cat > /tmp/lineage_airflow_scan.py << 'PYEOF'
"""Airflow codebase scanner — extracts DAG-to-table mappings."""
import argparse, json, os, re, sqlite3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dags-dir', required=True)
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--db-path', required=True)
    args = parser.parse_args()

    project_id = args.project_id
    escaped_pid = re.escape(project_id)

    TABLE_RE = re.compile(
        rf'{escaped_pid}\.([a-zA-Z_][a-zA-Z0-9_]+)\.([a-zA-Z_][a-zA-Z0-9_]+)'
    )
    DAG_ID_RE = re.compile(
        r'(?:dag_id\s*=\s*["\']([^"\']+)["\']'
        r'|DAG\(\s*["\']([^"\']+)["\'])'
    )
    CONFIG_VAR_RE = re.compile(r'Variable\.get\(\s*["\']([^"\']+)["\']')
    CONFIG_FILE_RE = re.compile(r'["\']([a-zA-Z][\w-]*\.json)["\']')
    SUBSCRIPT_RE = re.compile(
        r'Variable\.get\([^)]+\)\s*\[\s*\n?\s*["\']([^"\']+)["\']'
    )
    DAG_ID_BLOCKLIST = {'...', 'name', 'example'}

    def _filter_dag_ids(raw_matches):
        return [
            (g1 or g2).lower()
            for g1, g2 in raw_matches
            if (g1 or g2) not in DAG_ID_BLOCKLIST
            and len(g1 or g2) > 2
        ]

    def _classify_key(key):
        k = key.lower().replace('-', '_')
        if k in ('dag_id', 'dagid'):
            return 'dag_id'
        if 'dataset' in k or k.endswith('_ds') or k == 'ds':
            return 'dataset'
        if 'table' in k and k not in ('table_type', 'table_schema'):
            return 'table'
        return None

    def _deep_extract(obj, found):
        if isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(val, str) and val.strip():
                    kind = _classify_key(key)
                    if kind == 'dag_id':
                        found['dag_ids'].add(val.strip().lower())
                    elif kind == 'dataset':
                        found['datasets'].add(val.strip())
                    elif kind == 'table':
                        found['tables'].add(val.strip())
                _deep_extract(val, found)
        elif isinstance(obj, list):
            for item in obj:
                _deep_extract(item, found)

    def _empty_found():
        return {'dag_ids': set(), 'datasets': set(), 'tables': set()}

    def _extract_per_item(obj, global_datasets):
        pairs = []
        items_with_own_dag = []
        if not isinstance(obj, (dict, list)):
            return pairs, items_with_own_dag
        lists_to_check = []
        if isinstance(obj, list):
            lists_to_check.append(obj)
        elif isinstance(obj, dict):
            for val in obj.values():
                if isinstance(val, list):
                    lists_to_check.append(val)
                elif isinstance(val, dict):
                    for v2 in val.values():
                        if isinstance(v2, list):
                            lists_to_check.append(v2)
        for lst in lists_to_check:
            for item in lst:
                if not isinstance(item, dict):
                    continue
                item_found = _empty_found()
                _deep_extract(item, item_found)
                if item_found['dag_ids'] and item_found['tables']:
                    datasets = item_found['datasets'] or global_datasets
                    for did in item_found['dag_ids']:
                        items_with_own_dag.append(did)
                        for tbl in item_found['tables']:
                            for ds in datasets:
                                pairs.append((did, ds, tbl))
        return pairs, items_with_own_dag

    # Single filesystem walk
    py_files = {}
    sql_files = {}
    json_index = {}
    json_cache = {}

    for root_dir, dirs, files in os.walk(args.dags_dir):
        for f in files:
            filepath = os.path.join(root_dir, f)
            if f.endswith('.py'):
                try:
                    with open(filepath, 'r') as fh:
                        py_files[filepath] = fh.read()
                except Exception:
                    pass
            elif f.endswith('.sql'):
                try:
                    with open(filepath, 'r') as fh:
                        sql_files[filepath] = (root_dir, fh.read())
                except Exception:
                    pass
            elif f.endswith('.json'):
                stem = f.rsplit('.', 1)[0]
                normalized = re.sub(r'[_-]?(template|temp|sample|Template)$', '', stem)
                json_index[normalized.lower()] = filepath
                json_index[stem.lower()] = filepath
                try:
                    with open(filepath, 'r') as fh:
                        json_cache[filepath] = json.load(fh)
                except Exception:
                    pass

    # Source 1a: Python DAG files
    dag_table_map = {}
    dag_config_refs = {}
    dir_dag_ids = {}

    for filepath, content in py_files.items():
        root_dir = os.path.dirname(filepath)
        raw_matches = DAG_ID_RE.findall(content)
        dag_ids_in_file = _filter_dag_ids(raw_matches)
        for did in dag_ids_in_file:
            dir_dag_ids.setdefault(root_dir, set()).add(did)
        if not dag_ids_in_file:
            continue
        table_refs = TABLE_RE.findall(content)
        for dag_id in dag_ids_in_file:
            dag_table_map.setdefault(dag_id, set())
            for dataset, table in table_refs:
                dag_table_map[dag_id].add((dataset, table))
        var_names = CONFIG_VAR_RE.findall(content)
        file_refs = CONFIG_FILE_RE.findall(content)
        subscript_keys = SUBSCRIPT_RE.findall(content)
        for dag_id in dag_ids_in_file:
            for var_name in var_names:
                json_path = json_index.get(var_name.lower())
                if json_path:
                    sub_key = None
                    for sk in subscript_keys:
                        if sk.lower() != 'default':
                            sub_key = sk
                            break
                    dag_config_refs.setdefault(dag_id, []).append((json_path, sub_key))
            for fname in file_refs:
                full_path = os.path.join(root_dir, fname)
                if os.path.exists(full_path):
                    dag_config_refs.setdefault(dag_id, []).append((full_path, None))
                else:
                    stem = fname.rsplit('.', 1)[0].lower()
                    normalized = re.sub(r'[_-]?(template|temp|sample)$', '', stem)
                    jp = json_index.get(stem) or json_index.get(normalized)
                    if jp:
                        dag_config_refs.setdefault(dag_id, []).append((jp, None))

    # Source 1b: SQL files
    for filepath, (root_dir, content) in sql_files.items():
        table_refs = TABLE_RE.findall(content)
        if not table_refs:
            continue
        dag_ids = dir_dag_ids.get(root_dir) or dir_dag_ids.get(os.path.dirname(root_dir))
        if not dag_ids:
            continue
        for dag_id in dag_ids:
            dag_table_map.setdefault(dag_id, set())
            for dataset, table in table_refs:
                dag_table_map[dag_id].add((dataset, table))

    # Source 2a: JSON configs with own dag_id
    for json_path, data in json_cache.items():
        found = _empty_found()
        _deep_extract(data, found)
        if not found['tables']:
            continue
        per_item_pairs, _ = _extract_per_item(data, found['datasets'])
        if per_item_pairs:
            for did, ds, tbl in per_item_pairs:
                dag_table_map.setdefault(did, set()).add((ds, tbl))
        if found['dag_ids']:
            for did in found['dag_ids']:
                dag_table_map.setdefault(did, set())
                for tbl in found['tables']:
                    for ds in found['datasets']:
                        dag_table_map[did].add((ds, tbl))

    # Source 2b: Python->config linking
    for dag_id, refs in dag_config_refs.items():
        for json_path, subscript_key in refs:
            data = json_cache.get(json_path)
            if data is None:
                continue
            if subscript_key and isinstance(data, dict):
                section = data.get(subscript_key, {})
                found = _empty_found()
                _deep_extract(section, found)
                if 'default' in data and isinstance(data['default'], dict):
                    default_found = _empty_found()
                    _deep_extract(data['default'], default_found)
                    found['datasets'].update(default_found['datasets'])
            else:
                found = _empty_found()
                _deep_extract(data, found)
            if not found['tables'] or not found['datasets']:
                continue
            dag_table_map.setdefault(dag_id, set())
            for tbl in found['tables']:
                for ds in found['datasets']:
                    dag_table_map[dag_id].add((ds, tbl))

    # Deduplication: each (dataset, table) -> one dag_id
    table_candidates = {}
    for dag_id, tables in dag_table_map.items():
        for dataset, table in tables:
            table_candidates.setdefault((dataset, table), set()).add(dag_id)

    table_to_dag = {}
    for key, dag_ids in table_candidates.items():
        if len(dag_ids) == 1:
            table_to_dag[key] = next(iter(dag_ids))

    # Insert into SQLite
    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    for (dataset, table), dag_id in table_to_dag.items():
        # Skip if job_history already covers this (dataset, table, dag_id)
        cur.execute('''
            SELECT 1 FROM lineage
            WHERE target_dataset = ? AND target_name = ? AND pipeline_id = ?
              AND edge_source = 'job_history'
        ''', (dataset, table, dag_id))
        if cur.fetchone():
            skipped += 1
            continue
        cur.execute('''
            INSERT OR REPLACE INTO lineage
            (target_dataset, target_name, target_type, source_tables, pipeline_id,
             pipeline_type, edge_source, write_pattern, view_definition,
             first_seen, last_seen, job_count, user_email)
            VALUES (?, ?, 'TABLE', '[]', ?, 'airflow_dag', 'codebase_scan',
                    NULL, NULL, NULL, NULL, 0, NULL)
        ''', (dataset, table, dag_id))
        inserted += 1
    conn.commit()
    conn.close()
    print(f'Inserted {inserted} codebase_scan rows from {len(dag_table_map)} DAGs ({skipped} skipped — already in job_history)')

if __name__ == '__main__':
    main()
PYEOF

$VENV_PYTHON /tmp/lineage_airflow_scan.py \
  --dags-dir "<DAGS_DIR>" \
  --project-id "<PROJECT_ID>" \
  --db-path "<DB_PATH>"
```

Replace `<DAGS_DIR>`, `<PROJECT_ID>`, and `<DB_PATH>` with the resolved configuration values.

### If ORCHESTRATOR = generic

Write and execute a simpler Python script:

```bash
cat > /tmp/lineage_generic_scan.py << 'PYEOF'
"""Generic codebase scanner — maps files to BigQuery tables."""
import argparse, json, os, re, sqlite3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo-dir', required=True)
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--db-path', required=True)
    args = parser.parse_args()

    escaped_pid = re.escape(args.project_id)
    TABLE_RE = re.compile(
        rf'{escaped_pid}\.([a-zA-Z_][a-zA-Z0-9_]+)\.([a-zA-Z_][a-zA-Z0-9_]+)'
    )
    WRITE_RE = re.compile(
        r'(?:INSERT\s+INTO|MERGE\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|'
        r'load_table_from_dataframe|load_table_from_json|load_table_from_uri|'
        r'copy_table)',
        re.IGNORECASE
    )

    file_table_map = {}  # filepath -> set of (dataset, table)

    for root_dir, dirs, files in os.walk(args.repo_dir):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for f in files:
            if not f.endswith(('.py', '.sql', '.json', '.yaml', '.yml')):
                continue
            filepath = os.path.join(root_dir, f)
            try:
                with open(filepath, 'r') as fh:
                    content = fh.read()
            except Exception:
                continue
            table_refs = TABLE_RE.findall(content)
            if not table_refs:
                continue
            has_writes = bool(WRITE_RE.search(content))
            if has_writes:
                rel_path = os.path.relpath(filepath, args.repo_dir)
                file_table_map.setdefault(rel_path, set())
                for dataset, table in table_refs:
                    file_table_map[rel_path].add((dataset, table))

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()
    inserted = 0
    for filepath, tables in file_table_map.items():
        for dataset, table in tables:
            cur.execute('''
                INSERT OR REPLACE INTO lineage
                (target_dataset, target_name, target_type, source_tables, pipeline_id,
                 pipeline_type, edge_source, write_pattern, view_definition,
                 first_seen, last_seen, job_count, user_email)
                VALUES (?, ?, 'TABLE', '[]', ?, 'script', 'codebase_scan',
                        NULL, NULL, NULL, NULL, 0, NULL)
            ''', (dataset, table, filepath))
            inserted += 1
    conn.commit()
    conn.close()
    print(f'Inserted {inserted} codebase_scan rows from {len(file_table_map)} files')

if __name__ == '__main__':
    main()
PYEOF

$VENV_PYTHON /tmp/lineage_generic_scan.py \
  --repo-dir "<REPO_ROOT>" \
  --project-id "<PROJECT_ID>" \
  --db-path "<DB_PATH>"
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

Save the MCP query result as JSON. Write and execute this Python script:

```bash
cat > /tmp/lineage_view_parser.py << 'PYEOF'
"""Parse view definitions with sqlglot, insert into lineage SQLite."""
import json, sqlite3, sys
from datetime import datetime, timezone

try:
    import sqlglot
except ImportError:
    print("sqlglot not available — skipping view parsing")
    sys.exit(0)

def main():
    db_path = sys.argv[1]
    views_json = sys.argv[2]

    with open(views_json, 'r') as f:
        views = json.load(f)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    inserted = 0
    errors = 0

    for view in views:
        dataset = view['dataset']
        view_name = view['view_name']
        view_sql = view.get('view_definition', '')

        if not view_sql:
            continue

        try:
            statements = sqlglot.parse(view_sql, dialect='bigquery')
            source_tables = set()
            for stmt in statements:
                if stmt is None:
                    continue
                for table in stmt.find_all(sqlglot.exp.Table):
                    table_name = table.name
                    db = table.db
                    if table_name and not table_name.startswith('_'):
                        src_dataset = db if db else dataset
                        source_tables.add(f'{src_dataset}.{table_name}')

            if source_tables:
                source_json = json.dumps(sorted(source_tables))
                cur.execute('''
                    INSERT OR REPLACE INTO lineage
                    (target_dataset, target_name, target_type, source_tables,
                     pipeline_id, pipeline_type, edge_source, write_pattern,
                     view_definition, first_seen, last_seen, job_count, user_email)
                    VALUES (?, ?, 'VIEW', ?, NULL, NULL, 'view_definition',
                            NULL, ?, ?, ?, 1, NULL)
                ''', (dataset, view_name, source_json, view_sql, now, now))
                inserted += 1

        except Exception as e:
            errors += 1

    conn.commit()
    conn.close()
    print(f'Inserted {inserted} view_definition rows ({errors} parse errors)')

if __name__ == '__main__':
    main()
PYEOF

# Write the MCP query result to a temp file
echo '<VIEWS_JSON>' > /tmp/lineage_views.json

$VENV_PYTHON /tmp/lineage_view_parser.py "$DB_PATH" /tmp/lineage_views.json
```

Replace `<VIEWS_JSON>` with the JSON output from the MCP query in Step 7a. For large result sets, write the JSON to the temp file using a heredoc or Python.

---

## Step 8: Summary

Run these queries via Bash and display the results:

```bash
DB_PATH="<configured db_path>"

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
rm -f /tmp/lineage_airflow_scan.py /tmp/lineage_generic_scan.py /tmp/lineage_view_parser.py /tmp/lineage_views.json
```
