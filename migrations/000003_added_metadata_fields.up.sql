-- All metadata fields used by window types
CREATE TABLE metadata_fields (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL
);

-- Bridge table to handle metadata field references
CREATE TABLE metadata_fields_references (
    window_type_id BIGINT REFERENCES window_type(id) ON DELETE CASCADE,
    metadata_fields_id BIGINT REFERENCES metadata_fields(id) ON DELETE CASCADE,
    PRIMARY KEY (window_type_id, metadata_fields_id)
);

-- Materialised view combining window types with their metadata fields
CREATE MATERIALIZED VIEW window_type_metadata_fields AS
SELECT 
    wt.name AS window_type_name,
    wt.version AS window_type_version,
    mf.id AS metadata_field_id,
    mf.name AS metadata_field_name,
    mf.description AS metadata_field_description
FROM window_type wt
INNER JOIN metadata_fields_references mfr ON wt.id = mfr.window_type_id
INNER JOIN metadata_fields mf ON mfr.metadata_fields_id = mf.id
ORDER BY wt.name, wt.version, mf.name;

-- Function to refresh the materialised view
CREATE OR REPLACE FUNCTION refresh_window_type_metadata_fields()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW window_type_metadata_fields;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger on window_type table
CREATE TRIGGER trigger_refresh_window_type_metadata_fields_on_window_type
    AFTER INSERT OR UPDATE OR DELETE ON window_type
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_window_type_metadata_fields();

-- Trigger on metadata_fields table
CREATE TRIGGER trigger_refresh_window_type_metadata_fields_on_metadata_fields
    AFTER INSERT OR UPDATE OR DELETE ON metadata_fields
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_window_type_metadata_fields();

-- Trigger on the bridge table
CREATE TRIGGER trigger_refresh_window_type_metadata_fields_on_references
    AFTER INSERT OR UPDATE OR DELETE ON metadata_fields_references
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_window_type_metadata_fields();

-- Index to aid direct querying of metadata fields references table
CREATE INDEX idx_metadata_fields_references_id 
ON metadata_fields_references(metadata_fields_id);

-- Index to optimise lookups by window type name and version
CREATE INDEX idx_window_type_metadata_fields_lookup 
ON window_type_metadata_fields(window_type_name, window_type_version);

-- Index for reverse lookups by metadata field
CREATE INDEX idx_window_type_metadata_fields_field 
ON window_type_metadata_fields(metadata_field_name);


