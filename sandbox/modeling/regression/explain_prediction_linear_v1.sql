SELECT
  *
FROM
  ML.EXPLAIN_PREDICT(MODEL `sofia-data-305018.mlr_optimization.linear_reg_final_6_0`,
    (
    SELECT
      *
    FROM
      `sofia-data-305018.mlr_optimization.base_payments_provider_socios`), STRUCT(5 AS top_k_features))
ORDER BY ABS(predicted_subtotal - subtotal)
