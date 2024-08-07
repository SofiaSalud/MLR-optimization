CREATE OR REPLACE MODEL `sofia-data-305018.mlr_optimization.pca_embed_32`
OPTIONS(
  MODEL_TYPE='PCA',
  NUM_PRINCIPAL_COMPONENTS=32,
  SCALE_FEATURES=FALSE 
) 
AS

SELECT
  ml_generate_embedding_result
FROM 
  `sofia-data-305018.mlr_optimization.base_payments_provider_socios_embeddings`
