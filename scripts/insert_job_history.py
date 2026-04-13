"""Insert BigQuery job history JSON into lineage SQLite database."""
import json, sqlite3, sys

db_path = sys.argv[1]
data = json.loads(sys.stdin.read())
conn = sqlite3.connect(db_path)
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
