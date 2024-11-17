CREATE EXTENSION IF NOT EXISTS pg_search;

-- Core executions visibility table
-- Prefix introduced in order to be able migrate from advanced visibility based on btree_gin + fts to paradedb extension
-- TODO I am not sure if we can use here schemas, since permissions can be restrictive
--      so for now prefixes like pdb_%.
CREATE TABLE pdb_executions_visibility
(
    namespace_id           CHAR(64)     NOT NULL,
    run_id                 CHAR(64)     NOT NULL,
    start_time             TIMESTAMP    NOT NULL,
    execution_time         TIMESTAMP    NOT NULL,
    workflow_id            VARCHAR(255) NOT NULL,
    workflow_type_name     VARCHAR(255) NOT NULL,
    status                 INTEGER      NOT NULL, -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
    close_time             TIMESTAMP    NULL,
    history_length         BIGINT       NULL,
    history_size_bytes     BIGINT       NULL,
    execution_duration     BIGINT       NULL,
    state_transition_count BIGINT       NULL,
    memo                   BYTEA        NULL,
    encoding               VARCHAR(64)  NOT NULL,
    task_queue             VARCHAR(255) NOT NULL DEFAULT '',
    search_attributes      JSONB        NULL,
    parent_workflow_id     VARCHAR(255) NULL,
    parent_run_id          VARCHAR(255) NULL,
    root_workflow_id       VARCHAR(255) NOT NULL DEFAULT '',
    root_run_id            VARCHAR(255) NOT NULL DEFAULT '',
    PRIMARY KEY (namespace_id, run_id)
);

-- Main visibility index with fast fields
-- TODO WIP
CALL paradedb.create_bm25(
        index_name => 'pdb_executions_visibility_idx',
        table_name => 'pdb_executions_visibility',
        key_field => 'run_id',
        text_fields => ARRAY [
            paradedb.field('workflow_id'),
            paradedb.field('workflow_type_name'),
            paradedb.field('task_queue')
            ],
        numeric_fields => ARRAY [
            -- fast => true for fields frequently used in sorting/filtering
            paradedb.field('status', fast => true), -- Frequent status filtering
            paradedb.field('history_length', fast => true), -- Range queries and sorting
            paradedb.field('execution_duration', fast => true) -- Performance metrics sorting
            ],
        datetime_fields => ARRAY [
            -- fast => true for time-based queries and sorting
            paradedb.field('start_time', fast => true), -- Recent executions queries
            paradedb.field('execution_time', fast => true), -- Time-range filtering
            paradedb.field('close_time', fast => true) -- Completion time sorting
            ],
        json_fields => ARRAY [
            paradedb.field('search_attributes')
            ]
     );


