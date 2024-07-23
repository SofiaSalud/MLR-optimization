CREATE OR REPLACE TABLE `sofia-data-305018.mlr_optimization.base_payments_provider_socios` AS

WITH 
  service_agg AS (
    SELECT
      member_id, service_id, disease_case_id, sed.case_event_id, 
      
      MAX(health_plan_id) AS healthplan_id,
      MAX(provided_by_object_id) AS provided_by_object_id,
      MAX(related_service_id) AS related_service_id,
      MAX(process_state) AS process_state,
      MAX(service_type_value) AS service_type_value,
      MAX(provided_by_content_type_id) AS provided_by_content_type_id,

      MAX(prescription_items_member_checks) AS prescription_items_member_checks,
      MAX(medicines_check) AS medicines_check,
      MAX(prescription_items_json) AS prescription_items_json,
 
      MAX(services_date) AS services_date,
      MAX(caseevent_date) AS caseevent_date,
      MAX(diseasecase_date) AS diseasecase_date,
      MAX(med_note_date) AS med_note_date,

      MAX(diagnosis) AS diagnosis,
      MAX(disease_origin_diagnosis_icd) AS disease_origin_diagnosis_icd,
      MAX(diagnosis_icd) AS diagnosis_icd,
      MAX(diagnosis_description) AS diagnosis_description,
      MAX(case_event_origin_diagnosis_icd) AS case_event_origin_diagnosis_icd,
      MAX(cie_codes) AS cie_codes,
      MAX(cie_descriptions) AS cie_descriptions,
      MAX(cpt_codes) AS cpt_codes,
      MAX(cpt_descriptions) AS cpt_descriptions,
      MAX(cpt) AS cpt,
      MAX(evaluation_cie_keys) AS evaluation_cie_keys,
      MAX(evaluation_diagnostics) AS evaluation_diagnostics,
      MAX(evaluation_diagnostic_impression) AS evaluation_diagnostic_impression,
      MAX(specialization) AS specialization,

      MAX(diseasecase_description) AS diseasecase_description,

      MAX(service_description) AS service_description,
      MAX(services_comments) AS services_comments,
      MAX(specification) AS specification,
      MAX(servicecomment_comments) AS servicecomment_comments,

      MAX(caseevent_category) AS caseevent_category,
      MAX(event_class) AS event_class,
      MAX(caseevent_description) AS caseevent_description,
      MAX(eventcomment_comments) AS eventcomment_comments,
      MAX(caseeventsummary_category) AS caseeventsummary_category,

      MAX(medical_description) AS medical_description,
      MAX(medical_recommendation_notes) AS medical_recommendation_notes,
      MAX(observations) AS observations,
      MAX(interrogation_system) AS interrogation_system,
      MAX(interrogation_system_name) AS interrogation_system_name,
      MAX(exam_notes) AS exam_notes,
      MAX(health_summary) AS health_summary,
      MAX(suffering) AS suffering,
      MAX(med_note_type) AS med_note_type,
      MAX(med_note_motive) AS med_note_motive,
      MAX(med_explo_notes) AS med_explo_notes,
      MAX(therapy_notes) AS therapy_notes,
      MAX(pathologies_notes) AS pathologies_notes,
      MAX(surgeries_notes) AS surgeries_notes,
      MAX(vaccines_notes) AS vaccines_notes,
      MAX(hospitalizations_notes) AS hospitalizations_notes,
      MAX(allergies_notes) AS allergies_notes,
      MAX(malformations_notes) AS malformations_notes,
      MAX(medicines_notes) AS medicines_notes,
      MAX(evaluation_notes) AS evaluation_notes,

      MAX(sed.state) AS sed_state,
      MAX(emr.state) AS emr_state,

      MAX(discharge_ruling_notes) AS discharge_ruling_notes,
      MAX(administrative_ruling_notes) AS administrative_ruling_notes,
      MAX(internal_notes) AS internal_notes,
      MAX(relevant_history) AS relevant_history,

      MAX(medical_procedure_ruling_notes) AS medical_procedure_ruling_notes,
      MAX(quote_and_scheduling_ruling_notes) AS quote_and_scheduling_ruling_notes,
      MAX(discharge_reason) AS discharge_reason,

    FROM
      `sofia-data-305018.cadena_cuidado.services_events_diseases_socios` AS sed
    LEFT JOIN 
      `sofia-data-305018.cadena_cuidado.emr_socios` AS emr 
    USING(member_id, service_id, disease_case_id) 
    WHERE 
      process_state != 'CANCELLED'
    GROUP BY member_id, service_id, disease_case_id, case_event_id, health_plan_id
  ),
  service_clean AS (
    SELECT
      member_id, 
      service_id, 
      disease_case_id, 
      case_event_id,
      healthplan_id,
      services_date,
      provided_by_object_id,
      related_service_id,
      process_state,
      service_type_value,
      specialization,
      prescription_items_member_checks,
      med_note_type,
      medicines_check,
      prescription_items_json,
      CASE
        WHEN provided_by_content_type_id = 58 THEN 'out_of_network'
        WHEN provided_by_content_type_id = 59 THEN 'in_network'
        ELSE NULL
      END AS provider_network,

      IF(disease_origin_diagnosis_icd != 'None', disease_origin_diagnosis_icd, diagnosis_icd) AS icd,
      CASE 
        WHEN LOWER(diseasecase_description) != LOWER(medical_description) AND diseasecase_description != 'None' AND medical_description != 'None' THEN CONCAT(diseasecase_description, ' / ', medical_description)
        WHEN LOWER(diseasecase_description) = LOWER(medical_description) AND diseasecase_description != 'None' AND medical_description != 'None' THEN diseasecase_description
        WHEN diseasecase_description = 'None' AND medical_description != 'None' THEN medical_description
        WHEN diseasecase_description != 'None' AND medical_description = 'None' THEN diseasecase_description 
      END AS diseasecase_description,
      medical_recommendation_notes,
      cpt_codes,
      cpt_descriptions,
      cpt,
    FROM
      service_agg
  ),
  healthplan_healthplan AS (
    SELECT DISTINCT
      id,
      beneficiary_member_id
    FROM
      `sofia-data-305018.backend_db_20240702.healthplan_healthplan`
  ),
    providers_doctor AS (
      SELECT DISTINCT
        id,
        nickname, 
        CONCAT(first_name, ' ', first_last_name, ' ', second_last_name) AS member_name,
        descriptor, 
        is_primary_care_doctor,
        is_active, 
        is_pediatrics_care_doctor, 
        license_institution, 
        available_on_demand, 
        internal_notes, 
        availability_hours,
        specialization_fare_id
      FROM
        `sofia-data-305018.backend_db_20240702.providers_doctor`
    ),
    providers_specializationfare AS (
      SELECT DISTINCT
        id,
        name,
        medical_specialization_id
      FROM
        `sofia-data-305018.backend_db_20240702.providers_specializationfare` 
    ),
    providers_medicalspecialization AS (
      SELECT DISTINCT
        id, 
        name,
        description,
        type,
        is_primary_care
      FROM
        `sofia-data-305018.backend_db_20240702.providers_medicalspecialization` 
    ),
    app_member AS (
      SELECT DISTINCT
        id,
        residence_address_id,
        occupation,
        nickname,
        CONCAT(first_name, ' ', first_last_name, ' ', second_last_name) AS member_name,
        birth_country,
        height_cm,
        weight_kg,
        date_of_birth,
        birth_country_code,
        nationality_country_code,
        IF(user_id = represented_by_user_id OR (user_id IS NULL), represented_by_user_id, NULL) AS member_is_represented_by_user_id,
        rfc,
      FROM
        `sofia-data-305018.backend_db_20240702.sofia_app_member`
      WHERE
        deleted IS NULL
    ),
    providerbranchoffice AS (
      SELECT DISTINCT
        id,
        name,
        provider_id,
        admin_user_id,
      FROM
        `sofia-data-305018.backend_db_20240702.providers_providerbranchoffice`
    ),
    providers_provider AS (
      SELECT DISTINCT
        id,
        contract_category,
        category_id,
        website,
        state,
        notes
      FROM
        `sofia-data-305018.backend_db_20240702.providers_provider`
    ),
    providercategory AS (
      SELECT DISTINCT
        id,
        name
      FROM
        `sofia-data-305018.backend_db_20240702.providers_providercategory`
    ),
    app_user AS (
      SELECT DISTINCT
        id
      FROM
        `sofia-data-305018.backend_db_20240702.sofia_app_user`
    ),
    billinginfo AS (
      SELECT DISTINCT
        user_id,
        address_id
      FROM
        `sofia-data-305018.backend_db_20240702.payments_billinginfo`
    ),
    app_address AS (
      SELECT
        id,
        MAX(address1) AS address1,
        MAX(neighborhood) AS neighborhood,
        MAX(city) AS city,
        MAX(zipcode) AS zipcode,
        MAX(coordinates) AS coordinates
      FROM
        `sofia-data-305018.backend_db_20240702.sofia_app_address`
      WHERE
        deleted IS NULL
      GROUP BY 
        id
    ),
    location_location AS (
      SELECT
        zipcode,
        MAX(municipality) AS municipality,
        MAX(state_name) AS state_name,
        MAX(city) AS city
      FROM
        `sofia-data-305018.backend_db_20240702.location_location`
      WHERE
        is_available 
      GROUP BY 
        zipcode
    )


SELECT DISTINCT
  # parte medica del servicio
  service_payments.service_id,
  service_clean.case_event_id,
  service_payments.disease_case_id,
  service_clean.related_service_id,
  service_clean.process_state,
  service_payments.service_type,
  service_clean.service_type_value,
  service_clean.services_date,
  service_clean.icd,
  service_clean.diseasecase_description,
  service_clean.medical_recommendation_notes,
  service_clean.cpt_codes,
  service_clean.cpt_descriptions,
  service_clean.cpt,
  service_clean.prescription_items_member_checks,
  service_clean.med_note_type,
  service_clean.medicines_check,
  service_clean.prescription_items_json,

  
  # proveedor 
  providerbranchoffice.name AS provider_name,
  service_clean.provider_network,
  providers_provider.contract_category AS provider_contract_category,
  providercategory.name AS provider_category_name,
  providers_provider.website AS provider_website,
  providers_provider.state AS provider_state,
  providers_provider.notes AS provider_notes,
  app_address_provider.address1 AS provider_address1,
  app_address_provider.neighborhood AS provider_address_neighborhood,
  app_address_provider.city AS provider_address_city,
  app_address_provider.zipcode AS provider_address_zipcode,
  app_address_provider.coordinates AS provider_coordinates,
  location_location_provider.municipality AS provider_location_municipality,
  location_location_provider.state_name AS provider_location_state,
  location_location_provider.city AS provider_location_city,
  
  
  # informacion del doctor
  providers_doctor.nickname AS doctor_nickname, 
  providers_doctor.member_name AS doctor_name,
  providers_doctor.descriptor AS doctor_descriptor, 
  providers_doctor.is_primary_care_doctor AS doctor_is_primary_care_doctor,
  providers_doctor.is_active AS doctor_is_active, 
  providers_doctor.is_pediatrics_care_doctor AS doctor_is_pediatrics_care_doctor, 
  providers_doctor.license_institution AS doctor_license_institution, 
  providers_doctor.available_on_demand AS doctor_available_on_demand, 
  providers_doctor.internal_notes AS doctor_internal_notes, 
  providers_doctor.availability_hours AS doctor_availability_hours,
  service_clean.specialization AS service_specialization_name,
  providers_specializationfare.name AS providers_specializationfare_name,
  providers_medicalspecialization.name AS provider_specialization_name,
  providers_medicalspecialization.description AS provider_specialization_description,
  providers_medicalspecialization.type AS provider_specialization_type,
  providers_medicalspecialization.is_primary_care AS provider_specialization_is_primary_care,


  # personales del socio
  service_payments.member_id,
  service_payments.age_at_subscription AS member_age_at_subscription,
  service_payments.gender AS member_gender,
  app_member.occupation AS member_occupation,
  app_member.nickname AS member_nickname,
  app_member.member_name,
  app_member.birth_country AS member_birth_country,
  app_member.height_cm AS member_height_cm,
  app_member.weight_kg AS member_weight_kg,
  app_member.date_of_birth AS member_date_of_birth,
  app_member.birth_country_code AS member_birth_country_code,
  app_member.nationality_country_code AS member_nationality_country_code,
  app_member.member_is_represented_by_user_id,
  app_member.rfc AS member_rfc, 
  app_address_member.address1 AS member_address,
  app_address_member.neighborhood AS member_address_neighborhood,
  app_address_member.city AS member_address_city,
  app_address_member.zipcode AS member_address_zipcode,
  app_address_member.coordinates AS member_coordinates,
  location_location_member.municipality AS member_location_municipality,
  location_location_member.state_name AS member_location_state,
  location_location_member.city AS member_location_city,


  # primas
  service_payments.healthplan_id,
  service_payments.vc_schema, 
  service_payments.ipc_schema,
  service_payments.vertical,
  service_payments.is_internal,
  service_payments.is_collective,
  service_payments.product_id,
  service_payments.business_group_size,
  service_payments.business_group,
  service_payments.business_name,
  
  # datos economicos otros como medio de pago
  service_payments.transaction_id,
  service_payments.transaction_date,
  service_payments.transaction_type,
  service_payments.subtotal,
  service_payments.subtotal_cost_to_reinsurance,
  service_payments.reinsurance_participates,

	
FROM
  `sofia-data-305018.views_20240702.dashboards_ue_03_service_payments` AS service_payments 
LEFT JOIN
  service_clean
USING(member_id, service_id, disease_case_id, healthplan_id) 
LEFT JOIN
  healthplan_healthplan -- 145205
ON healthplan_healthplan.id = service_payments.healthplan_id AND healthplan_healthplan.beneficiary_member_id = service_payments.member_id
LEFT JOIN 
  providers_doctor -- 145205
ON providers_doctor.id = service_clean.provided_by_object_id
LEFT JOIN
  providers_specializationfare
ON providers_doctor.specialization_fare_id = providers_specializationfare.id
LEFT JOIN
  providers_medicalspecialization
ON providers_medicalspecialization.id = providers_specializationfare.medical_specialization_id
LEFT JOIN
  providerbranchoffice -- 145205
ON service_clean.provided_by_object_id = providerbranchoffice.id 
LEFT JOIN
  providers_provider -- 145205
ON providerbranchoffice.provider_id = providers_provider.id
LEFT JOIN 
  providercategory
ON providercategory.id = providers_provider.category_id
LEFT JOIN 
  app_user -- 145205
ON providerbranchoffice.admin_user_id = app_user.id
LEFT JOIN 
  billinginfo
ON billinginfo.user_id = app_user.id
LEFT JOIN 
  app_address AS app_address_provider
ON billinginfo.address_id = app_address_provider.id
LEFT JOIN 
  location_location AS location_location_provider
ON location_location_provider.zipcode = app_address_provider.zipcode
LEFT JOIN
  app_member
ON service_payments.member_id = app_member.id
LEFT JOIN 
  app_address AS app_address_member
ON app_member.residence_address_id = app_address_member.id
LEFT JOIN 
   location_location AS location_location_member
ON location_location_member.zipcode = app_address_member.zipcode
WHERE
  member_id IS NOT NULL

