-- ============================================================
-- Worker
-- ============================================================
-- name: RegisterWorker :one
INSERT INTO worker (
    public_key,
    connection_url)
VALUES (
    @public_key,
    @connection_url)
RETURNING
    id;

-- name: GetWorkerByID :one
SELECT
    id,
    public_key,
    connection_url,
    is_serving,
    created_at
FROM
    worker
WHERE
    id = @id;

-- name: CheckWorkerExistsById :one
SELECT
    EXISTS (
        SELECT
            1
        FROM
            worker
        WHERE
            id = @id);

-- name: SetWorkerServing :exec
UPDATE
    worker
SET
    is_serving = @is_serving
WHERE
    id = @id;

-- ============================================================
-- Nonce
-- ============================================================
-- name: CreateNonce :one
INSERT INTO worker_nonce (
    worker_id,
    nonce)
VALUES (
    @worker_id,
    @nonce)
RETURNING
    id,
    nonce,
    expires_at;

-- name: ConsumeNonce :one
UPDATE
    worker_nonce
SET
    used = TRUE
WHERE
    worker_id = @worker_id
    AND id = @nonce_id
    AND used = FALSE
    AND expires_at > now()
RETURNING
    id,
    nonce,
    worker_id;

-- name: DeleteExpiredNonces :exec
DELETE FROM worker_nonce
WHERE expires_at < now();

-- ============================================================
-- Session
-- ============================================================
-- name: CreateSession :one
INSERT INTO worker_session (
    worker_id,
    access_key,
    expires_at)
VALUES (
    @worker_id,
    @access_key,
    @expires_at)
RETURNING
    id,
    access_key,
    expires_at;

-- name: GetSession :one
SELECT
    id,
    worker_id,
    expires_at
FROM
    worker_session
WHERE
    access_key = @access_key
    AND revoked = FALSE
    AND expires_at > now();

-- name: RevokeSession :exec
UPDATE
    worker_session
SET
    revoked = TRUE
WHERE
    access_key = @access_key;

-- name: DeleteExpiredSessions :exec
DELETE FROM worker_session
WHERE expires_at < now();

-- ============================================================
-- Data Functions
-- ============================================================
-- name: CreateDataFunction :exec
INSERT INTO data_function (
    name,
    ast_hash,
    git_commit_hash,
    worker_id,
    output_model,
    is_active,
    input_model,
    execution_timeout_seconds,
    ttl_seconds)
VALUES (
    @name,
    @ast_hash,
    @git_commit_hash,
    @worker_id,
    @output_model,
    @is_active,
    @input_model,
    @execution_timeout_seconds,
    @ttl_seconds);

-- ============================================================
-- Task
-- ============================================================
-- name: CreateTask :exec
INSERT INTO task (
    name,
    ast_hash,
    worker_id,
    description,
    execution_timeout,
    deadline,
    retry_count,
    backoff_strategy,
    input_model,
    output_model,
    git_commit_hash,
    is_active)
VALUES (
    @name,
    @ast_hash,
    @worker_id,
    @description,
    @execution_timeout,
    @deadline,
    @retry_count,
    @backoff_strategy,
    @input_model,
    @output_model,
    @git_commit_hash,
    @is_active);

-- name: RequireDatafunctionForTask :exec
INSERT INTO task_required_data_function (
    task_name,
    task_ast_hash,
    task_worker_id,
    df_name,
    df_ast_hash,
    df_worker_id)
VALUES (
    @task_name,
    @task_ast_hash,
    @task_worker_id,
    @df_name,
    @df_ast_hash,
    @df_worker_id);

-- ============================================================
-- Workflows
-- ============================================================
-- name: CreateWorkflow :one
INSERT INTO workflow (
    name,
    hash,
    worker_id,
    source,
    description,
    input_model,
    task_concurrency_limit,
    halt_on_failure,
    git_commit_hash,
    status)
VALUES (
    @name,
    @hash,
    @worker_id,
    @source,
    @description,
    @input_model,
    @task_concurrency_limit,
    @halt_on_failure,
    @git_commit_hash,
    @status)
RETURNING
    id;

-- name: CreateWorkflowEdge :exec
INSERT INTO workflow_edges (
    workflow_id,
    from_task_name,
    from_task_ast_hash,
    from_task_worker_id,
    to_task_name,
    to_task_ast_hash,
    to_task_worker_id)
VALUES (
    @workflow_id,
    @from_task_name,
    @from_task_ast_hash,
    @from_task_worker_id,
    @to_task_name,
    @to_task_ast_hash,
    @to_task_worker_id);

-- name: CreateWorkflowTransistivepair :exec
INSERT INTO workflow_transistive_pairs (
    workflow_id,
    from_task_name,
    from_task_ast_hash,
    from_task_worker_id,
    to_task_name,
    to_task_ast_hash,
    to_task_worker_id)
VALUES (
    @workflow_id,
    @from_task_name,
    @from_task_ast_hash,
    @from_task_worker_id,
    @to_task_name,
    @to_task_ast_hash,
    @to_task_worker_id);

