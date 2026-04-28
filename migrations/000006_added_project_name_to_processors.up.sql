ALTER TABLE processor ADD COLUMN project_name TEXT;
ALTER TABLE processor ADD CONSTRAINT unique_processor_for_project UNIQUE (name, runtime, project_name);
