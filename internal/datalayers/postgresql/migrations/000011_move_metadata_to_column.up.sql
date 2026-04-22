BEGIN;

-- ============================================================
-- SNAPSHOT: capture pre-migration counts for verification
-- ============================================================

CREATE TEMP TABLE migration_checks AS
SELECT
  COUNT(*)                                            AS total_windows,
  COUNT(metadata)                                     AS windows_with_metadata,
  SUM(jsonb_object_keys(metadata) |> COUNT)           AS total_expected_keys
FROM windows
WHERE metadata IS NOT NULL;

-- More granular: snapshot every (window_id, key) pair we expect to migrate
CREATE TEMP TABLE expected_metadata AS
SELECT
  w.id              AS windows_id,
  w.window_type_id  AS window_type_id,
  kv.key            AS metadata_key,
  jsonb_typeof(kv.value) AS value_type
FROM windows w
CROSS JOIN LATERAL jsonb_each(w.metadata) AS kv(key, value)
WHERE w.metadata IS NOT NULL;

-- ============================================================
-- CREATE TABLE
-- ============================================================

CREATE TABLE metadata (
  windows_id      BIGINT,
  window_type_id  BIGINT,
  metadata_key    TEXT,
  result_value    DOUBLE PRECISION,
  result_array    DOUBLE PRECISION[],
  result_json     JSONB,
  PRIMARY KEY (windows_id, window_type_id, metadata_key),
  FOREIGN KEY (windows_id)     REFERENCES windows(id),
  FOREIGN KEY (window_type_id) REFERENCES window_type(id)
);

-- ============================================================
-- MIGRATE
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
-- CHECKS: verify nothing was lost before dropping the column
-- ============================================================

-- Check 1: row count matches expected keys
DO $$
DECLARE
  expected BIGINT;
  actual   BIGINT;
BEGIN
  SELECT COUNT(*) INTO expected FROM expected_metadata;
  SELECT COUNT(*) INTO actual   FROM metadata;

  IF actual <> expected THEN
    RAISE EXCEPTION 'Row count mismatch: expected % rows, got %', expected, actual;
  END IF;
END $$;

-- Check 2: every (windows_id, window_type_id, metadata_key) tuple is accounted for
DO $$
DECLARE
  missing BIGINT;
BEGIN
  SELECT COUNT(*) INTO missing
  FROM expected_metadata e
  WHERE NOT EXISTS (
    SELECT 1 FROM metadata m
    WHERE m.windows_id    = e.windows_id
      AND m.window_type_id = e.window_type_id
      AND m.metadata_key   = e.metadata_key
  );

  IF missing > 0 THEN
    RAISE EXCEPTION '% keys from windows.metadata were not migrated', missing;
  END IF;
END $$;

-- Check 3: no migrated row has all result columns NULL
-- (means a value type wasn't number/array/object - worth knowing about)
DO $$
DECLARE
  untyped BIGINT;
BEGIN
  SELECT COUNT(*) INTO untyped
  FROM metadata
  WHERE result_value IS NULL
    AND result_array IS NULL
    AND result_json  IS NULL;

  IF untyped > 0 THEN
    RAISE EXCEPTION '% metadata rows have no result data - unexpected value types exist', untyped;
  END IF;
END $$;

-- Check 4: no data was written to metadata for windows that had NULL metadata
DO $$
DECLARE
  ghost BIGINT;
BEGIN
  SELECT COUNT(*) INTO ghost
  FROM metadata m
  JOIN windows w ON w.id = m.windows_id
  WHERE w.metadata IS NULL;

  IF ghost > 0 THEN
    RAISE EXCEPTION '% metadata rows exist for windows with NULL metadata', ghost;
  END IF;
END $$;

-- ============================================================
-- SAFE TO DROP
-- ============================================================

ALTER TABLE windows DROP COLUMN metadata;

COMMIT;
