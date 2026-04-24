BEGIN;

-- ============================================================
-- 1. CREATE TABLE
-- ============================================================
CREATE TABLE metadata (
  windows_id     BIGINT,
  window_type_id BIGINT,
  metadata_key   TEXT,
  result_value   DOUBLE PRECISION,
  result_array   DOUBLE PRECISION[],
  result_json    JSONB,
  PRIMARY KEY (windows_id, window_type_id, metadata_key),
  FOREIGN KEY (windows_id)     REFERENCES windows(id),
  FOREIGN KEY (window_type_id) REFERENCES window_type(id),
  
  -- Automatically aborts the transaction if an unhandled JSON type 
  -- (like a string or boolean) tries to insert all NULLs.
  CONSTRAINT metadata_has_valid_type_chk CHECK (
    result_value IS NOT NULL OR 
    result_array IS NOT NULL OR 
    result_json  IS NOT NULL
  )
);

-- ============================================================
-- 2. MIGRATE
-- ============================================================
INSERT INTO metadata (windows_id, window_type_id, metadata_key, result_value, result_array, result_json)
SELECT
  w.id,
  w.window_type_id,
  kv.key,
  CASE WHEN jsonb_typeof(kv.value) = 'number' THEN (kv.value::TEXT)::DOUBLE PRECISION END,
  CASE WHEN jsonb_typeof(kv.value) = 'array'  THEN ARRAY(
    SELECT el::TEXT::DOUBLE PRECISION FROM jsonb_array_elements(kv.value) el
  ) END,
  CASE WHEN jsonb_typeof(kv.value) = 'object' THEN kv.value END
FROM windows w
CROSS JOIN LATERAL jsonb_each(w.metadata) AS kv(key, value)
WHERE w.metadata IS NOT NULL;

-- ============================================================
-- 3. SAFE TO DROP
-- ============================================================
ALTER TABLE windows DROP COLUMN metadata;

COMMIT;
