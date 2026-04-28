-- Drop indexes first
DROP INDEX IF EXISTS idx_annotation_window_types_window;
DROP INDEX IF EXISTS idx_annotation_algorithms_algorithm;
DROP INDEX IF EXISTS idx_annotations_time_range;

-- Drop junction tables first (due to foreign key constraints)
DROP TABLE IF EXISTS annotation_window_types;
DROP TABLE IF EXISTS annotation_algorithms;

-- Drop main table last
DROP TABLE IF EXISTS annotations;
