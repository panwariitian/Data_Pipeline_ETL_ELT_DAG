-- Rejected rows by file and reason, every run (filter on run_id for "today").
CREATE OR REPLACE TABLE mart_rejections AS
SELECT run_id, source_table AS source_file, reason AS failure_reason, count(*) AS rows_rejected
FROM (SELECT run_id, source_table, unnest(string_split(failure_reason, '; ')) AS reason FROM elt_rejected_rows)
GROUP BY 1, 2, 3 ORDER BY 1, 2, 4 DESC;
