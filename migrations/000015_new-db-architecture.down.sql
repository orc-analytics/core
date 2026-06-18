-- Indexes
DROP INDEX IF EXISTS workflow_transistive_pairs_hash_idx;

DROP INDEX IF EXISTS workflow_transistive_pairs_name_idx;

DROP INDEX IF EXISTS workflow_edges_hash_idx;

DROP INDEX IF EXISTS workflow_edges_name_idx;

DROP INDEX IF EXISTS workflow_registered_at_idx;

DROP INDEX IF EXISTS workflow_git_commit_idx;

DROP INDEX IF EXISTS workflow_hash_idx;

DROP INDEX IF EXISTS workflow_name_idx;

DROP INDEX IF EXISTS task_registered_at_idx;

DROP INDEX IF EXISTS task_git_commit_hash;

DROP INDEX IF EXISTS task_worker_idx;

DROP INDEX IF EXISTS data_function_registered_at_idx;

DROP INDEX IF EXISTS data_function_git_commit_hash;

DROP INDEX IF EXISTS data_function_worker_idx;

DROP INDEX IF EXISTS one_active_owner_per_workflow;

DROP INDEX IF EXISTS one_active_owner_per_task;

DROP INDEX IF EXISTS one_active_owner_per_data_function;

DROP INDEX IF EXISTS md5_hash_idx;

-- Tables
DROP TABLE IF EXISTS workflow_transistive_pairs;

DROP TABLE IF EXISTS workflow_edges;

DROP TABLE IF EXISTS workflow;

DROP TABLE IF EXISTS task_required_data_functions;

DROP TABLE IF EXISTS task;

DROP TABLE IF EXISTS data_function;

DROP TABLE IF EXISTS worker;

-- Enum types
DROP TYPE IF EXISTS backoff_strategy;

DROP TYPE IF EXISTS execution_status;

DROP TYPE IF EXISTS trigger_status;

DROP TYPE IF EXISTS registration_status;

DROP TYPE IF EXISTS trigger_source;

