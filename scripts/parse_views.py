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
