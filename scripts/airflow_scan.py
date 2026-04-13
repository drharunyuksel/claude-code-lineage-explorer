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
