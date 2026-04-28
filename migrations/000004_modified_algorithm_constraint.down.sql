SELECT 'IRREVERSIBLE MIGRATION - Manual rollback required.

This migration relaxed the uniqueness constraint from (name, version) to 
(name, version, window_type_id, processor_id), which may have allowed 
duplicate (name, version) pairs to be created, in the algorithms table.

TO MANUALLY ROLLBACK:
1. Identify duplicates: 
   SELECT name, version, COUNT(*) FROM algorithm GROUP BY name, version HAVING COUNT(*) > 1;

2. Decide which duplicate rows to keep/remove based on your business logic

3. Delete unwanted duplicates:
   -- Example: keep only the most recent row per (name, version)
   DELETE FROM algorithm WHERE id NOT IN (
     SELECT DISTINCT ON (name, version) id FROM algorithm 
     ORDER BY name, version, created_at DESC
   );

4. Recreate original constraint:
   ALTER TABLE algorithm ADD CONSTRAINT algorithm_name_version_key UNIQUE (name, version);

WARNING: Step 3 will cause permanent data loss. Backup your database first!' 
AS error_message;
