-- ============================================================
-- Worker
-- ============================================================
-- name: RegisterWorker :one
INSERT INTO worker (public_key, connection_url)
    VALUES (@public_key, @connection_url)
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
INSERT INTO worker_nonce (worker_id, nonce)
    VALUES (@worker_id, @nonce)
RETURNING
    id, nonce, expires_at;

-- name: ConsumeNonce :one
UPDATE
    worker_nonce
SET
    used = TRUE
WHERE
    nonce = @nonce
    AND worker_id = @worker_id
    AND used = FALSE
    AND expires_at > now()
RETURNING
    id,
    worker_id;

-- name: DeleteExpiredNonces :exec
DELETE FROM worker_nonce
WHERE expires_at < now();

-- ============================================================
-- Session
-- ============================================================
-- name: CreateSession :one
INSERT INTO worker_session (worker_id, access_key, expires_at)
    VALUES (@worker_id, @access_key, @expires_at)
RETURNING
    id, access_key, expires_at;

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

