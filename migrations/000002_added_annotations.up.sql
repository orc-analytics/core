-- Create annotations table
CREATE TABLE annotations (
  id BIGSERIAL PRIMARY KEY,
  time_from TIMESTAMP NOT NULL, 
  time_to TIMESTAMP NOT NULL,
  metadata JSONB,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create junction table for annotation -> algorithm relationships
CREATE TABLE annotation_algorithms (
  annotation_id BIGINT NOT NULL,
  algorithm_id BIGINT NOT NULL,
  PRIMARY KEY (annotation_id, algorithm_id),
  FOREIGN KEY (annotation_id) REFERENCES annotations(id) ON DELETE CASCADE,
  FOREIGN KEY (algorithm_id) REFERENCES algorithm(id) ON DELETE CASCADE
);

-- Create junction table for annotation -> window type relationships  
CREATE TABLE annotation_window_types (
  annotation_id BIGINT NOT NULL,
  window_type_id BIGINT NOT NULL,
  PRIMARY KEY (annotation_id, window_type_id),
  FOREIGN KEY (annotation_id) REFERENCES annotations(id) ON DELETE CASCADE,
  FOREIGN KEY (window_type_id) REFERENCES window_type(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX idx_annotations_time_range ON annotations(time_from, time_to);
CREATE INDEX idx_annotation_algorithms_algorithm ON annotation_algorithms(algorithm_id);
CREATE INDEX idx_annotation_window_types_window ON annotation_window_types(window_type_id);

-- Add comments for documentation
COMMENT ON TABLE annotations IS 'Plot annotations with time ranges and metadata';
COMMENT ON TABLE annotation_algorithms IS 'Junction table linking annotations to algorithms';
COMMENT ON TABLE annotation_window_types IS 'Junction table linking annotations to window types';

COMMENT ON COLUMN annotations.time_from IS 'Start time of the annotation time window';
COMMENT ON COLUMN annotations.time_to IS 'End time of the annotation time window';
COMMENT ON COLUMN annotations.description IS 'Detailed description of the annotation';
