
SELECT
  CENTROID_ID,

  ARRAY_AGG(DISTINCT service_type IGNORE NULLS LIMIT 10) AS array_service_type,
  ARRAY_AGG(DISTINCT services_specification IGNORE NULLS LIMIT 10) AS array_services_specification,
  ARRAY_AGG(DISTINCT diagnosis IGNORE NULLS LIMIT 10) AS array_diagnosis,
  ARRAY_AGG(DISTINCT provider_name IGNORE NULLS LIMIT 10) AS array_provider_name,
  ARRAY_AGG(DISTINCT caseevent_category IGNORE NULLS LIMIT 10) AS array_caseevent_category,
  ARRAY_AGG(DISTINCT provider_network IGNORE NULLS LIMIT 10) AS array_provider_network,
  ARRAY_AGG(DISTINCT provider_category_name IGNORE NULLS LIMIT 10) AS array_provider_category_name,
  ARRAY_AGG(DISTINCT doctor_descriptor IGNORE NULLS LIMIT 10) AS array_doctor_descriptor,
  ARRAY_AGG(DISTINCT cpt IGNORE NULLS LIMIT 10) AS array_cpt,
  ARRAY_AGG(DISTINCT business_group_size IGNORE NULLS LIMIT 10) AS array_business_group_size,
  ARRAY_AGG(DISTINCT vertical IGNORE NULLS LIMIT 10) AS array_vertical,
  ARRAY_AGG(DISTINCT member_gender IGNORE NULLS LIMIT 10) AS array_member_gender,
  --ARRAY_AGG(DISTINCT member_address_neighborhood IGNORE NULLS) AS array_member_address_neighborhood,
  --ARRAY_AGG(DISTINCT doctor_neighborhood IGNORE NULLS) AS array_doctor_neighborhood,
  --ARRAY_AGG(DISTINCT provider_address_neighborhood IGNORE NULLS) AS array_provider_address_neighborhood,
  ARRAY_AGG(DISTINCT member_range_age IGNORE NULLS LIMIT 10) AS array_member_range_age,
  --ARRAY_AGG(DISTINCT bmi_range IGNORE NULLS LIMIT 10) AS array_bmi_range,
  COUNT(DISTINCT member_id) AS q_member_id,
  COUNT(DISTINCT service_id) AS q_service_id,
  COUNT(DISTINCT case_event_id) AS q_case_event_id,
  COUNT(DISTINCT disease_case_id) AS q_disease_case_id,
  ROUND(SUM(subtotal)) AS subtotal
FROM(
SELECT
  CENTROID_ID,
  service_type,
  services_specification,
  diagnosis,
  caseevent_category,
  provider_network,
  provider_category_name,
  doctor_descriptor,
  cpt,
  provider_name,
  --doctor_name,
  --doctorfriend_name,
  business_group_size,
  vertical,
  member_gender,
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
  doctor_neighborhood,
  provider_address_neighborhood,
  member_id, service_id, case_event_id, disease_case_id, subtotal
FROM
  ML.PREDICT(MODEL `sofia-data-305018.mlr_optimization.kmeans_clustering`, 
    (
    SELECT 
      *
    FROM
      `sofia-data-305018.mlr_optimization.base_payments_provider_socios`
    WHERE
      DATE(transaction_date_max) BETWEEN DATE_SUB('2024-07-01', INTERVAL 1 YEAR) AND DATE_SUB('2024-07-01', INTERVAL 1 DAY)
    )
  )
)
GROUP BY
  CENTROID_ID
ORDER BY 
  --CENTROID_ID, subtotal DESC
  ARRAY_LENGTH(array_provider_network) 
