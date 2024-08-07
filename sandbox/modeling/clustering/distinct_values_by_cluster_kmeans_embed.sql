SELECT
  ROUND(AVG(len_array_service_type), 1) AS avg_len_array_service_type,
  ROUND(AVG(len_array_services_specification), 1) AS avg_len_array_services_specification,
  ROUND(AVG(len_array_caseevent_category), 1) AS avg_len_array_caseevent_category,
  ROUND(AVG(len_array_provider_network), 1) AS avg_len_array_provider_network,
  ROUND(AVG(len_array_provider_category_name), 1) AS avg_len_array_provider_category_name,
  ROUND(AVG(len_array_doctor_descriptor), 1) AS avg_len_array_doctor_descriptor,
  ROUND(AVG(len_array_cpt), 1) AS avg_len_array_cpt,
  ROUND(AVG(len_array_business_group_size), 1) AS avg_len_array_business_group_size,
  ROUND(AVG(len_array_vertical), 1) AS avg_len_array_vertical,
  ROUND(AVG(len_array_member_gender), 1) AS avg_len_array_member_gender,
  ROUND(AVG(len_array_member_address_neighborhood), 1) AS avg_len_array_member_address_neighborhood,
  ROUND(AVG(len_array_doctor_neighborhood), 1) AS avg_len_array_doctor_neighborhood,
  ROUND(AVG(len_array_provider_address_neighborhood), 1) AS avg_len_array_provider_address_neighborhood,
  ROUND(AVG(len_array_member_range_age), 1) AS avg_len_array_member_range_age,
  ROUND(AVG(len_array_bmi_range), 1) AS avg_len_array_bmi_range,
FROM(
SELECT
  CENTROID_ID,

  ARRAY_LENGTH(ARRAY_AGG(DISTINCT service_type IGNORE NULLS)) AS len_array_service_type,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT services_specification IGNORE NULLS)) AS len_array_services_specification,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT caseevent_category IGNORE NULLS)) AS len_array_caseevent_category,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT provider_network IGNORE NULLS)) AS len_array_provider_network,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT provider_category_name IGNORE NULLS)) AS len_array_provider_category_name,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT doctor_descriptor IGNORE NULLS)) AS len_array_doctor_descriptor,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT cpt IGNORE NULLS)) AS len_array_cpt,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT business_group_size IGNORE NULLS)) AS len_array_business_group_size,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT vertical IGNORE NULLS)) AS len_array_vertical,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT member_gender IGNORE NULLS)) AS len_array_member_gender,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT member_address_neighborhood IGNORE NULLS)) AS len_array_member_address_neighborhood,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT doctor_neighborhood IGNORE NULLS)) AS len_array_doctor_neighborhood,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT provider_address_neighborhood IGNORE NULLS)) AS len_array_provider_address_neighborhood,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT member_range_age IGNORE NULLS)) AS len_array_member_range_age,
  ARRAY_LENGTH(ARRAY_AGG(DISTINCT bmi_range IGNORE NULLS)) AS len_array_bmi_range,
  COUNT(DISTINCT member_id) AS q_member_id,
  COUNT(DISTINCT service_id) AS q_service_id,
  COUNT(DISTINCT case_event_id) AS q_case_event_id,
  COUNT(DISTINCT disease_case_id) AS q_disease_case_id,
  ROUND(SUM(subtotal)) AS subtotal
FROM(
SELECT
  CENTROID_ID,
  service_type,
  COUNT(*) OVER(PARTITION BY service_type) AS freq_service_type,
  services_specification,
  COUNT(*) OVER(PARTITION BY services_specification) AS freq_services_specification,
  caseevent_category,
  COUNT(*) OVER(PARTITION BY caseevent_category) AS freq_caseevent_category,
  provider_network,
  COUNT(*) OVER(PARTITION BY provider_network) AS freq_provider_network,
  provider_category_name,
  COUNT(*) OVER(PARTITION BY provider_category_name) AS freq_provider_category_name,
  doctor_descriptor,
  COUNT(*) OVER(PARTITION BY doctor_descriptor) AS freq_doctor_descriptor,
  cpt,
  COUNT(*) OVER(PARTITION BY cpt) AS freq_cpt,
  --provider_name,
  --doctor_name,
  --doctorfriend_name,
  business_group_size,
  COUNT(*) OVER(PARTITION BY business_group_size) AS freq_business_group_size,
  vertical,
  COUNT(*) OVER(PARTITION BY vertical) AS freq_vertical,
  member_gender,
  COUNT(*) OVER(PARTITION BY member_gender) AS freq_member_gender,
  CASE
    WHEN member_age_at_subscription BETWEEN 0 AND 2 THEN 'Infants and Toddlers (0-2)'
    WHEN member_age_at_subscription BETWEEN 3 AND 5 THEN 'Early Childhood (3-5)'
    WHEN member_age_at_subscription BETWEEN 6 AND 12 THEN 'Children (6-12)'
    WHEN member_age_at_subscription BETWEEN 13 AND 18 THEN 'Adolescents (13-18)'
    WHEN member_age_at_subscription BETWEEN 19 AND 34 THEN 'Young Adults (19-34)'
    WHEN member_age_at_subscription BETWEEN 35 AND 49 THEN 'Adults (35-49)'
    WHEN member_age_at_subscription BETWEEN 50 AND 64 THEN 'Middle-Aged Adults (50-64)'
    WHEN member_age_at_subscription BETWEEN 65 AND 79 THEN 'Seniors (65-79)'
    WHEN member_age_at_subscription >= 80 THEN 'Elderly (80+)'
    ELSE 'Unknown'
  END AS member_range_age,
  CASE
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) < 18.5 THEN 'Underweight'
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) BETWEEN 18.5 AND 24.9 THEN 'Normal weight'
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) BETWEEN 25 AND 29.9 THEN 'Overweight'
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) BETWEEN 30 AND 34.9 THEN 'Obesity Class I (Moderate)'
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) BETWEEN 35 AND 39.9 THEN 'Obesity Class II (Severe)'
    WHEN 10000*CAST(member_weight_kg AS INT64)/(CAST(member_height_cm AS INT64)*CAST(member_height_cm AS INT64)) >= 40 THEN 'Obesity Class III (Very severe or morbid obesity)'
    ELSE 'Unknown'
  END AS bmi_range,
  member_address_neighborhood,
  COUNT(*) OVER(PARTITION BY member_address_neighborhood) AS freq_member_address_neighborhood,
  doctor_neighborhood,
  COUNT(*) OVER(PARTITION BY doctor_neighborhood) AS freq_doctor_neighborhood,
  provider_address_neighborhood,
  COUNT(*) OVER(PARTITION BY provider_address_neighborhood) AS freq_provider_address_neighborhood,
  member_id, service_id, case_event_id, disease_case_id, subtotal
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
)
GROUP BY
  CENTROID_ID
--ORDER BY 
--  CENTROID_ID, subtotal DESC
)