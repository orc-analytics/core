CREATE TYPE window_state AS ENUM (
    'PENDING',              -- processing is waiting for other conditions to be fufilled
    'FAILED',               -- whether there was an issue after registering the window
    'PROCESSING',           -- algorithms are running on this window
    'PROCESSING_FINISHED'   -- all processing has finished
);
CREATE TYPE algorithm_state AS ENUM (
    'PENDING',               -- the algorithm is scheduled to run for this window
    'PROCESSING',            -- the algorithm is running
    'SUCCEEDED',             -- the algorithm suceeded
    'FAILED_HANDLED',        -- the algorithm failed in a handled way
    'FAILED_UNHANDLED'       -- the algorithm failed in an unhandled way
);
ALTER TABLE windows ADD COLUMN state window_state NOT NULL DEFAULT 'PENDING';
ALTER TABLE results ADD COLUMN state algorithm_state NOT NULL DEFAULT 'SUCCEEDED';
ALTER TABLE results ADD COLUMN error JSONB; -- only populated when state = 'HANDLED_FAILED' OR 'UNHANDLED_FAILED' - not enforced at DB level
