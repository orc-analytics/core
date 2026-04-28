-- Drop views
DROP MATERIALIZED VIEW IF EXISTS algorithm_execution_paths;

-- Drop triggers first
DROP TRIGGER IF EXISTS refresh_algorithm_execution_paths_after_algorithm_change ON algorithm;
DROP TRIGGER IF EXISTS refresh_algorithm_execution_paths_after_dependency_change ON algorithm_dependency;

-- Drop function
DROP FUNCTION IF EXISTS refresh_algorithm_exec_paths;

-- Drop tables in reverse order of dependencies
DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS windows;
DROP TABLE IF EXISTS algorithm_dependency;
DROP TABLE IF EXISTS algorithm;
DROP TABLE IF EXISTS processor;
DROP TABLE IF EXISTS window_type;

-- Drop extension
DROP EXTENSION IF EXISTS ltree;
DROP TYPE IF EXISTS result_type;
