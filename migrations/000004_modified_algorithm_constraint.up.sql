-- Drop the existing unique constraint on (name, version)
ALTER TABLE algorithm DROP CONSTRAINT algorithm_name_version_key;

-- Add new unique constraint on (name, version, window_type_id, processor_id)
ALTER TABLE algorithm ADD CONSTRAINT algorithm_name_version_window_processor_key 
  UNIQUE (name, version, window_type_id, processor_id);
