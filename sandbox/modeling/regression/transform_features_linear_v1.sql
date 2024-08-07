SELECT DISTINCT 
  business_group_size_coded,
  business_group_size
FROM 
  `sofia-data-305018.mlr_optimization.base_payments_provider_socios` AS a
JOIN(
  SELECT
    member_id, service_id, disease_case_id,
    business_group_size AS business_group_size_coded
  FROM
    ML.TRANSFORM(
      MODEL `sofia-data-305018.mlr_optimization.linear_reg`,
      TABLE `sofia-data-305018.mlr_optimization.base_payments_provider_socios`)
)
USING(member_id, service_id, disease_case_id)
WHERE
  DATE(transaction_date_max) BETWEEN DATE_SUB('2024-07-01', INTERVAL 1 YEAR) AND DATE_SUB('2024-07-01', INTERVAL 1 DAY)
ORDER BY business_group_size_coded 
