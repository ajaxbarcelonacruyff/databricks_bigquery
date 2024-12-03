with ga AS(
  SELECT * FROM temp_bigquery_ga4_tables  -- change this view name to the view you created in the previous cell
)
SELECT  ga.*
FROM ga
;
