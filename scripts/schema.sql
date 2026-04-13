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
