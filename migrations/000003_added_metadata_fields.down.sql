-- Drop indexes on materialised view (if they exist)
DROP INDEX IF EXISTS idx_window_type_metadata_fields_field;
DROP INDEX IF EXISTS idx_window_type_metadata_fields_lookup;

-- Drop index on bridge table
DROP INDEX IF EXISTS idx_metadata_fields_references_id;

-- Drop triggers
DROP TRIGGER IF EXISTS trigger_refresh_window_type_metadata_fields_on_references 
ON metadata_fields_references;

DROP TRIGGER IF EXISTS trigger_refresh_window_type_metadata_fields_on_metadata_fields 
ON metadata_fields;

DROP TRIGGER IF EXISTS trigger_refresh_window_type_metadata_fields_on_window_type 
ON window_type;

-- Drop the trigger function
DROP FUNCTION IF EXISTS refresh_window_type_metadata_fields();

-- Drop the materialized view
DROP MATERIALIZED VIEW IF EXISTS window_type_metadata_fields;

-- Drop the bridge table
DROP TABLE IF EXISTS metadata_fields_references;

-- Drop the metadata fields table
DROP TABLE IF EXISTS metadata_fields;
