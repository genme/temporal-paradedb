-- Core executions visibility table
CREATE TABLE pdb_executions_visibility (
  namespace_id            CHAR(64)      NOT NULL,
  run_id                  CHAR(64)      NOT NULL,
  start_time              DATETIME(6)   NOT NULL,
  execution_time          DATETIME(6)   NOT NULL,
  workflow_id             VARCHAR(255)  NOT NULL,
  workflow_type_name      VARCHAR(255)  NOT NULL,
  status                  INT           NOT NULL,
  close_time              DATETIME(6)   NULL,
  history_length          BIGINT        NULL,
  history_size_bytes      BIGINT        NULL,
  execution_duration      BIGINT        NULL,
  state_transition_count  BIGINT        NULL,
  memo                    BLOB          NULL,
  search_attributes       JSON          NULL,
  task_queue              VARCHAR(255)  NOT NULL DEFAULT '',
  parent_workflow_id      VARCHAR(255)  NULL,
  parent_run_id           VARCHAR(255)  NULL,
  root_workflow_id        VARCHAR(255)  NOT NULL DEFAULT '',
  root_run_id             VARCHAR(255)  NOT NULL DEFAULT '',
  PRIMARY KEY (namespace_id, run_id)
);

-- Search attributes table
CREATE TABLE pdb_custom_search_attributes (
  namespace_id      CHAR(64)  NOT NULL,
  run_id            CHAR(64)  NOT NULL,
  search_attributes JSON      NULL,
  PRIMARY KEY (namespace_id, run_id)
);

-- Main visibility index with fast fields
CALL paradedb.create_bm25(
  index_name => 'pdb_executions_visibility_idx',
  table_name => 'pdb_executions_visibility',
  key_field => 'run_id',
  text_fields => ARRAY[
    paradedb.field('workflow_id'),
    paradedb.field('workflow_type_name'),
    paradedb.field('task_queue')
  ],
  numeric_fields => ARRAY[
    -- fast => true for fields frequently used in sorting/filtering
    paradedb.field('status', fast => true),          -- Frequent status filtering
    paradedb.field('history_length', fast => true),  -- Range queries and sorting
    paradedb.field('execution_duration', fast => true) -- Performance metrics sorting
  ],
  datetime_fields => ARRAY[
    -- fast => true for time-based queries and sorting
    paradedb.field('start_time', fast => true),       -- Recent executions queries
    paradedb.field('execution_time', fast => true),   -- Time-range filtering
    paradedb.field('close_time', fast => true)        -- Completion time sorting
  ],
  json_fields => ARRAY[
    paradedb.field('search_attributes')
  ]
);


-- Custom search attributes index - simple JSON indexing
CALL paradedb.create_bm25(
  index_name => 'pdb_custom_search_attributes_idx',
  table_name => 'pdb_custom_search_attributes',
  key_field => 'run_id',
  json_fields => ARRAY[
    paradedb.field('search_attributes')
  ]
);
