---------------------- Core Operations ----------------------  
-- name: CreateProcessor :exec
INSERT INTO processor (
  name,
  runtime,
  connection_string,
  project_name
) VALUES (
  sqlc.arg('name'),
  sqlc.arg('runtime'),
  sqlc.arg('connection_string'),
  sqlc.arg('project_name')
) ON CONFLICT (name, runtime) DO UPDATE 
SET 
  name = EXCLUDED.name,
  runtime = EXCLUDED.runtime,
  connection_string = EXCLUDED.connection_string,
  project_name = EXCLUDED.project_name
RETURNING id;

-- name: CreateMetadataField :one
INSERT INTO metadata_fields (
  name,
  description
) VALUES (
  sqlc.arg('name'),
  sqlc.arg('description')
) ON CONFLICT (name) DO UPDATE
SET
  name = EXCLUDED.name,
  description = EXCLUDED.description
RETURNING id;

-- name: CreateWindowType :one
INSERT INTO window_type (
  name,
  version,
  description
) VALUES (
  sqlc.arg('name'),
  sqlc.arg('version'),
  sqlc.arg('description')
) ON CONFLICT (name, version) DO UPDATE
SET
  name = EXCLUDED.name,
  version = EXCLUDED.version,
  description = EXCLUDED.description
RETURNING id;

-- name: CreateWindowTypeMetadataFieldBridge :exec
INSERT INTO metadata_fields_references (
  window_type_id,
  metadata_fields_id
) VALUES (
  sqlc.arg('window_type_id'),
  sqlc.arg('metadata_fields_id')
) ON CONFLICT (window_type_id, metadata_fields_id) DO UPDATE
SET 
  window_type_id = EXCLUDED.window_type_id,
  metadata_fields_id = EXCLUDED.metadata_fields_id;

-- name: CreateAlgorithm :exec
WITH processor_id AS (
  SELECT id FROM processor p
  WHERE p.name = sqlc.arg('processor_name') 
  AND p.runtime = sqlc.arg('processor_runtime')
),
window_type_id AS (
  SELECT id FROM window_type w
  WHERE w.name = sqlc.arg('window_type_name') 
  AND w.version = sqlc.arg('window_type_version')
)
INSERT INTO algorithm (
  name,
  version,
  description,
  processor_id,
  window_type_id,
  result_type,
  self_lookback_count, 
  self_lookback_timedelta,
  self_lookback_gap_count,
  self_lookback_gap_timedelta
) VALUES (
  sqlc.arg('name'),
  sqlc.arg('version'),
  sqlc.arg('description'),
  (SELECT id FROM processor_id),
  (SELECT id FROM window_type_id),
  sqlc.arg('result_type'),
  sqlc.arg('self_lookback_count'),
  sqlc.arg('self_lookback_timedelta'),
  sqlc.arg('self_lookback_gap_count'),
  sqlc.arg('self_lookback_gap_timedelta')
) ON CONFLICT DO NOTHING ;

-- name: ReadAlgorithmsForWindow :many
SELECT a.* FROM algorithm a
JOIN window_type wt ON a.window_type_id = wt.id
WHERE wt.name = sqlc.arg('window_type_name') 
AND wt.version = sqlc.arg('window_type_version');

-- name: ReadAlgorithms :many
SELECT a.* FROM algorithm a;

-- name: ReadAlgorithmsForProcessorId :many
SELECT a.* FROM algorithm a
WHERE a.processor_id = sqlc.arg('processor_id');

-- name: CreateAlgorithmDependency :exec
WITH from_algo AS (
  SELECT a.id, a.window_type_id, a.processor_id FROM algorithm a
  JOIN processor p ON a.processor_id = p.id
  WHERE a.name = sqlc.arg('from_algorithm_name')
  AND a.version = sqlc.arg('from_algorithm_version')
  AND p.name = sqlc.arg('from_processor_name')
  AND p.runtime = sqlc.arg('from_processor_runtime')
),
to_algo AS (
  SELECT a.id, a.window_type_id, a.processor_id FROM algorithm a
  JOIN processor p ON a.processor_id = p.id
  WHERE a.name = sqlc.arg('to_algorithm_name')
  AND a.version = sqlc.arg('to_algorithm_version')
  AND p.name = sqlc.arg('to_processor_name')
  AND p.runtime = sqlc.arg('to_processor_runtime')
)
INSERT INTO algorithm_dependency (
  from_algorithm_id,
  to_algorithm_id,
  from_window_type_id,
  to_window_type_id,
  from_processor_id,
  to_processor_id,
  lookback_count,
  lookback_timedelta,
  lookback_gap_count,
  lookback_gap_timedelta
) VALUES (
  (SELECT id FROM from_algo LIMIT 1),
  (SELECT id FROM to_algo LIMIT 1),
  (SELECT window_type_id FROM from_algo LIMIT 1),
  (SELECT window_type_id FROM to_algo LIMIT 1),
  (SELECT processor_id FROM from_algo LIMIT 1),
  (SELECT processor_id FROM to_algo LIMIT 1),
  sqlc.arg('lookback_count'),
  sqlc.arg('lookback_timedelta'),
  sqlc.arg('lookback_gap_count'),
  sqlc.arg('lookback_gap_timedelta')
) ON CONFLICT (from_algorithm_id, to_algorithm_id) DO UPDATE
  SET
    from_window_type_id = excluded.from_window_type_id,
    to_window_type_id = excluded.to_window_type_id,
    from_processor_id = excluded.from_processor_id,
    to_processor_id = excluded.to_processor_id,
    lookback_count = excluded.lookback_count,
    lookback_timedelta = excluded.lookback_timedelta;

-- name: ReadFromAlgorithmDependencies :many
WITH from_algo AS (
  SELECT a.id, a.window_type_id, a.processor_id FROM algorithm a
  JOIN processor p ON a.processor_id = p.id
  WHERE a.name = sqlc.arg('from_algorithm_name')
  AND a.version = sqlc.arg('from_algorithm_version')
  AND p.name = sqlc.arg('from_processor_name')
  AND p.runtime = sqlc.arg('from_processor_runtime')
)
SELECT ad.* FROM algorithm_dependency ad WHERE ad.from_algorithm_id = from_algo.id;

-- name: ReadAlgorithmId :one
WITH processor_id AS (
  SELECT p.id FROM processor p
  WHERE p.name = sqlc.arg('processor_name')
  AND p.runtime = sqlc.arg('processor_runtime')
)
SELECT a.id FROM algorithm a
WHERE a.name = sqlc.arg('algorithm_name')
AND a.version = sqlc.arg('algorithm_version')
AND a.processor_id = (SELECT id from processor_id);
  
-- name: ReadAlgorithmExecutionPaths :many
SELECT aep.* FROM algorithm_execution_paths aep WHERE aep.window_type_id_path ~ ('*.' || sqlc.arg('window_type_id')::TEXT || '.*')::lquery;

-- name: ReadAlgorithmExecutionPathsForAlgo :many
SELECT aep.* FROM algorithm_execution_paths aep WHERE aep.final_algo_id=sqlc.arg('algo_id');

-- name: ReadWindowTypes :many
SELECT wt.* FROM window_type wt;

-- name: RegisterWindow :one
WITH window_type_id AS (
  SELECT id FROM window_type 
  WHERE name = sqlc.arg('window_type_name') 
  AND version = sqlc.arg('window_type_version')
)
INSERT INTO windows (
  window_type_id,
  time_from, 
  time_to,
  origin
) VALUES (
  (SELECT id FROM window_type_id),
  sqlc.arg('time_from'),
  sqlc.arg('time_to'),
  sqlc.arg('origin')
) RETURNING window_type_id, id;

-- name: CreateMetadata :exec
INSERT INTO metadata (
  windows_id,
  window_type_id,
  metadata_key,
  result_value,
  result_array,
  result_json
) VALUES (
  sqlc.arg('windows_id'),
  sqlc.arg('window_type_id'),
  sqlc.arg('metadata_key'),
  sqlc.arg('result_value'),
  sqlc.arg('result_array'),
  sqlc.arg('result_json')
) ON CONFLICT (windows_id, window_type_id, metadata_key) DO UPDATE
SET
  result_value = EXCLUDED.result_value,
  result_array = EXCLUDED.result_array,
  result_json  = EXCLUDED.result_json;

-- name: CreateResult :one
INSERT INTO results (
  windows_id,
  window_type_id, 
  algorithm_id, 
  result_value,
  result_array,
  result_json
) VALUES (
  sqlc.arg('windows_id'),
  sqlc.arg('window_type_id'),
  sqlc.arg('algorithm_id'),
  sqlc.arg('result_value'),
  sqlc.arg('result_array'),
  sqlc.arg('result_json')
) RETURNING id;

-- name: ReadProcessors :many
SELECT * FROM processor;

-- name: ReadProcessorExcludeProject :many
SELECT * FROM processor WHERE project_name != sqlc.arg('project_name');

-- name: ReadProcessorsByIDs :many
SELECT *
FROM processor
WHERE id = ANY(sqlc.arg('processor_ids')::bigint[])
ORDER BY name, runtime;

-- name: ReadMetadataFieldsByWindowType :many
SELECT 
    metadata_field_id,
    metadata_field_name,
    metadata_field_description
FROM window_type_metadata_fields
WHERE window_type_name = sqlc.arg('window_type_name')
  AND window_type_version = sqlc.arg('window_type_version')
ORDER BY metadata_field_name;

-- name: ReadMetadataFields :many
SELECT * FROM metadata_fields;

-- name: ReadWindowTypeMetadataFields :many
SELECT * FROM window_type_metadata_fields;

-- name: ReadResultsForAlgorithmByTimedelta :many
SELECT
    r.id as result_id,
    r.algorithm_id,
    w.id as window_id,
    a.result_type,
    r.result_value, 
    r.result_array,
    r.result_json,
    wt.name as window_type_name, 
    wt.version as window_type_version,
    w.time_from as window_time_from,
    w.time_to as window_time_to,
    w.origin as window_origin,
    (
      SELECT jsonb_object_agg(m.metadata_key, 
        COALESCE(
          to_jsonb(m.result_value),
          to_jsonb(m.result_array),
          m.result_json
        )
      )
      FROM metadata m
      WHERE m.windows_id = w.id
    ) AS window_metadata
FROM results r
JOIN windows w ON w.id = r.windows_id
JOIN window_type wt ON wt.id = w.window_type_id
JOIN algorithm a ON a.id = r.algorithm_id 
WHERE
    r.algorithm_id = sqlc.arg('algorithm_id')
    AND w.time_from > sqlc.arg('search_from')
    AND w.time_to < sqlc.arg('search_to')
ORDER BY w.time_from, w.time_to DESC
LIMIT sqlc.narg('limit')
OFFSET sqlc.arg('count_offset');

-- name: ReadResultsForAlgorithmByCount :many
SELECT
    r.id as result_id,
    r.algorithm_id,
    w.id as window_id,
    a.result_type,
    r.result_value, 
    r.result_array,
    r.result_json,
    wt.name as window_type_name, 
    wt.version as window_type_version,
    w.time_from as window_time_from,
    w.time_to as window_time_to,
    w.origin as window_origin,
    (
      SELECT jsonb_object_agg(m.metadata_key, 
        COALESCE(
          to_jsonb(m.result_value),
          to_jsonb(m.result_array),
          m.result_json
        )
      )
      FROM metadata m
      WHERE m.windows_id = w.id
    ) AS window_metadata
FROM results r
JOIN windows w ON w.id = r.windows_id
JOIN window_type wt ON wt.id = w.window_type_id
JOIN algorithm a ON a.id = r.algorithm_id
WHERE
    r.algorithm_id = sqlc.arg('algorithm_id')
    AND w.time_to < sqlc.arg('search_to')
ORDER BY w.time_from, w.time_to DESC
LIMIT sqlc.narg('limit')
OFFSET sqlc.arg('count_offset');
