SELECT
  CENTROID_ID,
  FORMAT("%'d", CAST(SUM(subtotal) AS INT64)) AS subtotal_str
FROM
  ML.PREDICT(MODEL `sofia-data-305018.mlr_optimization.kmeans_clustering_embed`,
    (
    SELECT 
  *
FROM(
SELECT * FROM ML.PREDICT(
  MODEL `sofia-data-305018.mlr_optimization.pca_embed_32`, (
SELECT 
  *
FROM 
  `sofia-data-305018.mlr_optimization.base_payments_provider_socios`
LEFT JOIN(
  SELECT
    member_id, 
    service_id, 
    disease_case_id,
    ml_generate_embedding_result,
  FROM
    `sofia-data-305018.mlr_optimization.base_payments_provider_socios_embeddings`
)
USING(member_id, service_id, disease_case_id)
WHERE
  DATE(transaction_date_max) BETWEEN DATE_SUB('2024-07-01', INTERVAL 1 YEAR) AND DATE_SUB('2024-07-01', INTERVAL 1 DAY)
)
)
)
    )
  )
GROUP BY 1
ORDER BY SUM(subtotal) DESC

