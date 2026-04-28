DROP MATERIALIZED VIEW IF EXISTS algorithm_execution_paths;

CREATE MATERIALIZED VIEW algorithm_execution_paths AS
WITH RECURSIVE leaf_nodes AS (
  -- leaf nodes
    SELECT
        algorithm_dependency.to_algorithm_id
    FROM
        algorithm_dependency
    EXCEPT
    SELECT
        from_algorithm_id
    FROM
        algorithm_dependency
),
search_tree AS (
    -- root nodes
    SELECT
        a.id AS algo_id,
        0 AS num_dependencies,
        a.id::VARCHAR AS algo_id_path,
        a.processor_id::VARCHAR AS proc_id_path,
        a.window_type_id::VARCHAR as window_type_id_path,
        '0'::VARCHAR AS lookback_count_path,
        '0'::VARCHAR AS lookback_timedelta_path
    FROM
        algorithm a
    WHERE
        a.id NOT IN (
            SELECT ad.to_algorithm_id
            FROM algorithm_dependency ad
        )

    UNION ALL

    SELECT
        ad.to_algorithm_id AS algo_id,
        st.num_dependencies + 1,
        st.algo_id_path || '.' || ad.to_algorithm_id::VARCHAR,
        st.proc_id_path || '.' || ad.to_processor_id::VARCHAR,
        st.window_type_id_path || '.' || ad.to_window_type_id::VARCHAR,
        st.lookback_count_path || '.' || ad.lookback_count::VARCHAR,
        st.lookback_timedelta_path || '.' || ad.lookback_timedelta::VARCHAR
    FROM
        algorithm_dependency ad
    JOIN
        search_tree st ON ad.from_algorithm_id = st.algo_id
),
final_view AS (
    SELECT
        st.algo_id AS final_algo_id,
        st.num_dependencies,
        text2ltree(st.algo_id_path) AS algo_id_path,
        text2ltree(st.window_type_id_path) AS window_type_id_path,
        text2ltree(st.proc_id_path) AS proc_id_path,
        text2ltree(st.lookback_count_path) as lookback_count_path,
        text2ltree(st.lookback_timedelta_path) as lookback_timedelta_path
    FROM search_tree st
    WHERE
        st.algo_id IN (SELECT to_algorithm_id FROM leaf_nodes)
        OR st.num_dependencies = 0 -- no dependencies
    ORDER BY nlevel(text2ltree(st.algo_id_path))
)
SELECT * FROM final_view;
