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
