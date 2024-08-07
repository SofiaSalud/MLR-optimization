SELECT 
  *
FROM
  ML.GENERATE_EMBEDDING(
    MODEL `sofia-data-305018.cadena_cuidado.embedding-multilingual-text-vertex`,
    (
      SELECT 
        member_id, 
        service_id, 
        disease_case_id, 
        CONCAT(
          'service type: ', IFNULL(service_type, ''),  
          ', service specification: ', IFNULL(services_specification, ''), 
          ', service category: ', IFNULL(caseevent_category, ''), 
          ', service class: ', IFNULL(caseevent_class, ''), 
          ', provider category name: ', IFNULL(provider_category_name, ''), 
          ', provider network: ', IFNULL(provider_network, ''), 
          ', procedure CPT (Current Procedural Terminology): ', IFNULL(cpt, ''), 
          ', diagnosis: ', IFNULL(diagnosis, ''), 
          ', is a external doctor?: ', IF(doctorfriend_name IS NOT NULL, 'yes', 'no'), 
          ', doctor specialties: ', IFNULL(doctor_descriptor, ''), 
          ', patient occupation: ', IFNULL(member_occupation, ''), 
          ', patient weight kg: ', IFNULL(member_weight_kg, -1), 
          ', patient height cm: ', IFNULL(member_height_cm, -1), 
          ', patient gender: ', IFNULL(member_gender, ''), 
          ', patient age: ', IFNULL(member_age_at_subscription, -1)
        ) AS content
      FROM `sofia-data-305018.mlr_optimization.base_payments_provider_socios`
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
)
