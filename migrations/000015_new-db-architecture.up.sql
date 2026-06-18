-- ============================================================================
-- PostgreSQL backend schema
-- ============================================================================
-- Maps the gRPC contract (core.proto / worker.proto / shared.proto) onto a
-- Postgres database that mirrors the various services as closely as possible,
-- whilst also enabling the various requirements.
--
-- There are two halves to the database model:
--
--   * Registry  - DataFunction / Task / Workflow, definitions
--   * Runtime   - workflow runs, task results, data-function executions.
--
-- ============================================================================
-- Enum types. Map enums defined in the protobuf schema closely.
-- ============================================================================
CREATE TYPE trigger_source AS ENUM (
    'TRIGGER_SOURCE_UNSPECIFIED',
    'TRIGGER_SOURCE_CRON',
    'TRIGGER_SOURCE_WEBHOOK',
    'TRIGGER_SOURCE_UI',
    'TRIGGER_SOURCE_CLI'
);

CREATE TYPE registration_status AS ENUM (
    'REGISTRATION_STATUS_UNSPECIFIED',
    'REGISTRATION_STATUS_SUCCESSFUL',
    'REGISTRATION_STATUS_FAILED'
);

CREATE TYPE trigger_status AS ENUM (
    'TRIGGER_STATUS_UNSPECIFIED',
    'TRIGGER_STATUS_ACCEPTED',
    'TRIGGER_STATUS_REJECTED'
);

CREATE TYPE execution_status AS ENUM (
    'EXECUTION_STATUS_UNSPECIFIED',
    'EXECUTION_STATUS_SUCCESSFUL',
    'EXECUTION_STATUS_FAILED'
);

CREATE TYPE backoff_strategy AS ENUM (
    'BACKOFF_STRATEGY_LINEAR',
    'BACKOFF_STRATEGY_EXPONENTIAL'
);

-- ============================================================================
-- REGISTRY
-- ============================================================================
-- Workers
--
-- Defines all the unique workers that assets (tasks, datafunctions)
-- are owned by. Names are globally unique.
CREATE TABLE worker (
    name text NOT NULL, -- name of the worker, globally unique - always
    md5_hash text NOT NULL, -- a unique hash combining the assets defined in the worker. mutable.
    connection_url text NOT NULL, -- the connection URL to contact the worker
    PRIMARY KEY (name)
);

-- for easy access of connection URL on hash
CREATE INDEX md5_hash_idx ON worker (md5_hash);

CREATE TABLE data_function (
    name text NOT NULL, -- name of the data function
    ast_hash text NOT NULL, -- the has of the AST segment
    git_commit_hash text NOT NULL, -- git commit hash of the current commit
    worker_name text NOT NULL REFERENCES worker (name) ON DELETE CASCADE, -- name of the worker that owns it
    output_model jsonb NOT NULL,
    is_served boolean DEFAULT TRUE, -- a flag that if set to true, denotes
    -- that this version is currently being served
    input_model jsonb,
    execution_timeout_seconds integer,
    ttl_seconds integer,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, ast_hash, worker_name)
    -- ensures that a DF can be attached to different workers
    -- at different moments in time.
);

-- Tasks
CREATE TABLE task (
    name text NOT NULL,
    ast_hash text NOT NULL, -- hash of the AST for the segment that defines the task
    worker_name text NOT NULL REFERENCES worker (name) ON DELETE CASCADE,
    description text NOT NULL,
    execution_timeout integer, -- seconds
    deadline integer, -- seconds
    retry_count integer,
    backoff_strategy backoff_strategy DEFAULT 'BACKOFF_STRATEGY_LINEAR',
    input_model jsonb,
    output_model jsonb,
    git_commit_hash text NOT NULL, -- current git commit
    is_served boolean DEFAULT TRUE, -- a flag that if set to true, denotes
    -- that this version is currently being served
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, ast_hash, worker_name) -- ensures that a task can be attached to different workers
    -- at different moments in time.
);

-- what tasks require what data functions
CREATE TABLE task_required_data_functions (
    task_name text NOT NULL,
    task_ast_hash text NOT NULL,
    task_worker_name text NOT NULL,
    df_name text NOT NULL,
    df_ast_hash text NOT NULL,
    df_worker_name text NOT NULL,
    FOREIGN KEY (task_name, task_ast_hash, task_worker_name) REFERENCES task (name, ast_hash, worker_name) ON DELETE CASCADE,
    FOREIGN KEY (df_name, df_ast_hash, df_worker_name) REFERENCES data_function (name, ast_hash, worker_name) ON DELETE CASCADE,
    PRIMARY KEY (task_name, task_ast_hash, task_worker_name, df_name, df_ast_hash, df_worker_name)
);

-- workflow definitions
CREATE TABLE workflow (
    name text NOT NULL,
    hash text NOT NULL,
    worker_name text NOT NULL,
    description text,
    input_model jsonb,
    task_concurrency_limit integer,
    halt_on_failure boolean,
    git_commit_hash text NOT NULL, -- current git commit
    is_served boolean,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, hash, worker_name) -- workflows are globally unique, and are simply defined by their name and contents. We need to track who defined them to that we know when to de-serve them
);

-- defines the edges to workflows
CREATE TABLE workflow_edges (
    workflow_name text NOT NULL,
    workflow_hash text NOT NULL,
    workflow_worker_name text NOT NULL,
    from_task_name text NOT NULL,
    from_task_ast_hash text NOT NULL,
    from_task_worker_name text NOT NULL,
    to_task_name text NOT NULL,
    to_task_ast_hash text NOT NULL,
    to_task_worker_name text NOT NULL,
    FOREIGN KEY (workflow_name, workflow_hash, workflow_worker_name) REFERENCES workflow (name, hash, worker_name),
    FOREIGN KEY (from_task_name, from_task_ast_hash, from_task_worker_name) REFERENCES task (name, ast_hash, worker_name) ON DELETE CASCADE,
    FOREIGN KEY (to_task_name, to_task_ast_hash, to_task_worker_name) REFERENCES task (name, ast_hash, worker_name) ON DELETE CASCADE,
    PRIMARY KEY (workflow_name, workflow_hash, workflow_worker_name, from_task_name, from_task_ast_hash, from_task_worker_name, to_task_name, to_task_ast_hash, to_task_worker_name)
);

-- transistive pairs. contains edges between each task that will eventually reach
-- another task.
-- enables O(1) lookup of cycle detection. e.g. the transistive pairs of:
-- A -> B -> C -> D would be:
-- - A -> B
-- - A -> C
-- - A -> D
-- - B -> C
-- - B -> D
-- - C -> D
-- Then if a new edge is introduced that would complete a cycle (e.g. D->A), then you check for the inverse:
-- A->D - this exists, so the new pair would introduce a cycle.
-- search indices
CREATE TABLE workflow_transistive_pairs (
    workflow_name text NOT NULL,
    workflow_hash text NOT NULL,
    workflow_worker_name text NOT NULL,
    from_task_name text NOT NULL,
    from_task_ast_hash text NOT NULL,
    from_task_worker_name text NOT NULL,
    to_task_name text NOT NULL,
    to_task_ast_hash text NOT NULL,
    to_task_worker_name text NOT NULL,
    FOREIGN KEY (workflow_name, workflow_hash, workflow_worker_name) REFERENCES workflow (name, hash, worker_name),
    FOREIGN KEY (from_task_name, from_task_ast_hash, from_task_worker_name) REFERENCES task (name, ast_hash, worker_name) ON DELETE CASCADE,
    FOREIGN KEY (to_task_name, to_task_ast_hash, to_task_worker_name) REFERENCES task (name, ast_hash, worker_name) ON DELETE CASCADE,
    PRIMARY KEY (workflow_name, workflow_hash, workflow_worker_name, from_task_name, from_task_ast_hash, from_task_worker_name, to_task_name, to_task_ast_hash, to_task_worker_name)
);

-- partial indices ensure that no two data functions, tasks and workflows with the same
-- name can exist that are both served
CREATE UNIQUE INDEX one_active_owner_per_data_function ON data_function (name)
WHERE
    is_served = TRUE;

CREATE UNIQUE INDEX one_active_owner_per_task ON task (name)
WHERE
    is_served = TRUE;

CREATE UNIQUE INDEX one_active_owner_per_workflow ON workflow (name)
WHERE
    is_served = TRUE;

-- indexes for search performance
CREATE INDEX data_function_worker_idx ON data_function (worker_name);

CREATE INDEX data_function_git_commit_hash ON data_function (git_commit_hash);

CREATE INDEX data_function_registered_at_idx ON data_function (registered_at);

CREATE INDEX task_worker_idx ON task (worker_name);

CREATE INDEX task_git_commit_hash ON task (git_commit_hash);

CREATE INDEX task_registered_at_idx ON task (registered_at);

CREATE INDEX workflow_name_idx ON workflow (name);

CREATE INDEX workflow_hash_idx ON workflow (hash);

CREATE INDEX workflow_git_commit_idx ON workflow (git_commit_has);

CREATE INDEX workflow_registered_at_idx ON workflow (registered_at);

CREATE INDEX workflow_edges_name_idx ON workflow_edges (workflow_name);

CREATE INDEX workflow_edges_hash_idx ON workflow_edges (workflow_hash);

CREATE INDEX workflow_transistive_pairs_name_idx ON workflow_transistive_pairs (workflow_name);

CREATE INDEX workflow_transistive_pairs_hash_idx ON workflow_transistive_pairs (workflow_hash);

