SELECT
  a.processed_input,
  AVG(COALESCE(a.weight, cat.weight)) AS weight,
  MAX_BY(cat.category, COALESCE(a.weight, cat.weight))
FROM
  ML.WEIGHTS(MODEL `sofia-data-305018.mlr_optimization.linear_reg`,
    STRUCT(true AS standardize)) AS a
LEFT JOIN
UNNEST(category_weights) AS cat
GROUP BY a.processed_input
ORDER BY 
  weight DESC
