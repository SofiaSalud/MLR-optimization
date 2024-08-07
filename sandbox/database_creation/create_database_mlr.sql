CREATE OR REPLACE TABLE `sofia-data-305018.mlr_optimization.base_payments_provider_socios` AS
-- ver si es necesario agregar informacion de medicamentos
-- agrupar en un solo campo el mismo sentido

WITH 
  base AS (
		SELECT DISTINCT
			sed.member_id,
			sed.service_id,

			sed.case_event_id,
			sed.disease_case_id,

			services_related_service_id,
			services_health_plan_id,

      services_created_date,

      IF(caseevent_admission_source = 'None', NULL, caseevent_admission_source) AS caseevent_admission_source,
      IF(caseevent_category = 'None', NULL, caseevent_category) AS caseevent_category,
      IF(caseevent_class = 'None', NULL, caseevent_class) AS caseevent_class,

      IF(services_service_type_value = 'None', NULL, services_service_type_value) AS services_service_type_value,
      IF(med_note_type = 'None', NULL, med_note_type) AS med_note_type,
      services_provided_by_content_type_id,
      services_provided_by_object_id,
      IF(doctor_specialization = 'None', NULL, doctor_specialization) AS doctor_specialization,
      IF(services_process_state = 'None', NULL, services_process_state) AS services_process_state,
      IF(services_diagnosis_icd = 'None', NULL, services_diagnosis_icd) AS services_diagnosis_icd, 
      IF(caseeventfinalletterdata_cie_codes = 'None', NULL, caseeventfinalletterdata_cie_codes) AS caseeventfinalletterdata_cie_codes, 
      IF(caseeventfinalletterdata_cie_descriptions = 'None', NULL, caseeventfinalletterdata_cie_descriptions) AS caseeventfinalletterdata_cie_descriptions,
      IF(disease_origin_diagnosis_icd = 'None', NULL, disease_origin_diagnosis_icd) AS disease_origin_diagnosis_icd, 
      IF(evaluation_cie_keys = 'None', NULL, evaluation_cie_keys) AS evaluation_cie_keys,
      IF(services_cpt = 'None', NULL, services_cpt) AS services_cpt, 
      IF(caseeventfinalletterdata_cpt_codes = 'None', NULL, caseeventfinalletterdata_cpt_codes) AS caseeventfinalletterdata_cpt_codes,
      IF(caseeventfinalletterdata_cpt_descriptions = 'None', NULL, caseeventfinalletterdata_cpt_descriptions) AS caseeventfinalletterdata_cpt_descriptions,
      IF(caseeventfinalletterdata_observations = 'None', NULL, caseeventfinalletterdata_observations) AS caseeventfinalletterdata_observations,
      IF(diagnosis = 'None', NULL, diagnosis) AS diagnosis,
      IF(diseasecase_description = 'None', NULL, diseasecase_description) AS diseasecase_description,
      IF(services_diagnosis_description = 'None' OR services_diagnosis_description = '', NULL, services_diagnosis_description) AS services_diagnosis_description,
			IF(services_service_description = 'None', NULL, services_service_description) AS services_service_description,
      IF(services_specification = 'None', NULL, services_specification) AS services_specification,
      IF(specialization = 'None', NULL, specialization) AS specialization,
      IF(interrogation_system = 'None', NULL, interrogation_system) AS interrogation_system,
      IF(med_note_motive = 'None' OR med_note_motive = '', NULL, med_note_motive) AS med_note_motive,
      --IF(suffering = 'None', NULL, suffering) AS suffering,

		FROM
			`sofia-data-305018.cadena_cuidado.services_events_diseases_socios` AS sed
		LEFT JOIN 
			`sofia-data-305018.cadena_cuidado.emr_socios` AS emr
		USING(member_id, service_id)
  ),
  clean_base AS (
    SELECT
      member_id,
			service_id,

			MAX_BY(case_event_id, services_created_date) AS case_event_id,
			MAX_BY(disease_case_id, services_created_date) AS disease_case_id,

      MAX_BY(services_created_date, services_created_date) AS services_created_date,

      MAX_BY(caseevent_admission_source, services_created_date) AS caseevent_admission_source,
      MAX_BY(caseevent_category, services_created_date) AS caseevent_category,
      MAX_BY(caseevent_class, services_created_date) AS caseevent_class,

			MAX_BY(services_related_service_id, services_created_date) AS related_service_id,
			services_health_plan_id AS healthplan_id,
      MAX_BY(services_service_type_value, services_created_date) AS services_service_type_value,
      COALESCE(MAX_BY(services_service_type_value, services_created_date), MAX_BY(med_note_type, services_created_date)) AS service_type,
      MAX_BY(services_provided_by_content_type_id, services_created_date) AS provided_by_content_type_id,
      CASE
        WHEN MAX_BY(services_provided_by_content_type_id, services_created_date) = 57 THEN 'hospital-pharmacy-lab'
        WHEN MAX_BY(services_provided_by_content_type_id, services_created_date) = 58 THEN 'doctor_out_of_network'
        WHEN MAX_BY(services_provided_by_content_type_id, services_created_date) = 59 THEN 'doctor_in_network'
        ELSE CAST(MAX_BY(services_provided_by_content_type_id, services_created_date) AS STRING)
      END AS provider_network,
      MAX_BY(services_provided_by_object_id, services_created_date) AS provided_by_object_id,
      MAX_BY(doctor_specialization, services_created_date) AS doctor_specialization,
      CASE
        WHEN MAX_BY(specialization, services_created_date) = "AGY" THEN "ALGOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ANY" THEN "ANESTESIOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ANG" THEN "ANGIOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "BRA" THEN "BARIATRÍA Y COMORBILIDADES"
        WHEN MAX_BY(specialization, services_created_date) = "HRB" THEN "BIOLOGÍA REPRODUCTIVA HUMANA"
        WHEN MAX_BY(specialization, services_created_date) = "CRY" THEN "CARDIOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ICA" THEN "CARDIOLOGÍA INTERVENCIONISTA"
        WHEN MAX_BY(specialization, services_created_date) = "PCA" THEN "CARDIOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "JNS" THEN "CIRUGÍA ARTICULAR"
        WHEN MAX_BY(specialization, services_created_date) = "BRS" THEN "CIRUGÍA BARIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "CRS" THEN "CIRUGÍA CARDIOTORÁCICA"
        WHEN MAX_BY(specialization, services_created_date) = "SPS" THEN "CIRUGÍA DE COLUMNA"
        WHEN MAX_BY(specialization, services_created_date) = "COS" THEN "CIRUGÍA DE CÓRNEA"
        WHEN MAX_BY(specialization, services_created_date) = "EPS" THEN "CIRUGÍA DE EPILEPSIA"
        WHEN MAX_BY(specialization, services_created_date) = "HAS" THEN "CIRUGÍA DE MANO"
        WHEN MAX_BY(specialization, services_created_date) = "RTA" THEN "CIRUGÍA DE RETINA Y VÍTREO"
        WHEN MAX_BY(specialization, services_created_date) = "TRS" THEN "CIRUGÍA DE TRASPLANTES"
        WHEN MAX_BY(specialization, services_created_date) = "DRS" THEN "CIRUGÍA DERMATOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "HMG" THEN "HEMATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "MFM" THEN "MEDICINA FETAL Y MATERNA"
        WHEN MAX_BY(specialization, services_created_date) = "GNS" THEN "CIRUGÍA GENERAL"
        WHEN MAX_BY(specialization, services_created_date) = "HPB" THEN "CIRUGÍA HEPATOPANCREATOBILIAR"
        WHEN MAX_BY(specialization, services_created_date) = "ONS" THEN "CIRUGÍA ONCOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "PSO" THEN "ONCOLOGÍA QUIRÚRGICA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "PDR" THEN "CIRUGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "PLA" THEN "CIRUGÍA PLÁSTICA Y RECONSTRUCTIVA"
        WHEN MAX_BY(specialization, services_created_date) = "RFS" THEN "CIRUGÍA REFRACTIVA"
        WHEN MAX_BY(specialization, services_created_date) = "DRY" THEN "DERMATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PDD" THEN "DERMATOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "DRP" THEN "DERMATOPATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ECL" THEN "ECOCARDIOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ENY" THEN "ENDOCRINOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PEN" THEN "ENDOCRINOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "GYE" THEN "ENDOSCOPIA GINECOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "EDO" THEN "ENFERMEDADES EXTERNAS DEL OJO Y SUPERFICIE OCULAR"
        WHEN MAX_BY(specialization, services_created_date) = "PHY" THEN "FISIOTERAPIA Y REHABILITACIÓN"
        WHEN MAX_BY(specialization, services_created_date) = "PHS" THEN "FONOCIRUGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "GSY" THEN "GASTROENTEROLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PDG" THEN "GASTROENTEROLOGÍA PEDIÁTRICA Y NUTRICIÓN"
        WHEN MAX_BY(specialization, services_created_date) = "MDG" THEN "GENÉTICA MÉDICA"
        WHEN MAX_BY(specialization, services_created_date) = "PRG" THEN "GENÉTICA PERINATAL"
        WHEN MAX_BY(specialization, services_created_date) = "GRS" THEN "GERIATRÍA"
        WHEN MAX_BY(specialization, services_created_date) = "GYA" THEN "GINECOLOGÍA Y OBSTETRICIA"
        WHEN MAX_BY(specialization, services_created_date) = "HPT" THEN "HEPATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "IMG" THEN "IMAGENOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "DGI" THEN "IMAGENOLOGÍA DIAGNÓSTICA Y TERAPÉUTICA"
        WHEN MAX_BY(specialization, services_created_date) = "LRL" THEN "LARINGOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "INM" THEN "MEDICINA INTERNA"
        WHEN MAX_BY(specialization, services_created_date) = "GNM" THEN "MEDICINA GENERAL"
        WHEN MAX_BY(specialization, services_created_date) = "NCM" THEN "MEDICINA NUCLEAR"
        WHEN MAX_BY(specialization, services_created_date) = "ASM" THEN "MICROCIRUGÍA DEL SEGMENTO ANTERIOR"
        WHEN MAX_BY(specialization, services_created_date) = "MOT" THEN "MOTILIDAD"
        WHEN MAX_BY(specialization, services_created_date) = "NPY" THEN "NEFROLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PNP" THEN "NEFROLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "NNY" THEN "NEONATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PDP" THEN "NEUMOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "NRS" THEN "NEUROCIRUGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "CLR" THEN "NEUROFISIOLOGÍA CLÍNICA"
        WHEN MAX_BY(specialization, services_created_date) = "NRY" THEN "NEUROLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "NRP" THEN "NEUROPEDIATRÍA"
        WHEN MAX_BY(specialization, services_created_date) = "CNP" THEN "NEUROPSICOLOGÍA CLÍNICA"
        WHEN MAX_BY(specialization, services_created_date) = "CLN" THEN "NUTRICIÓN CLÍNICA"
        WHEN MAX_BY(specialization, services_created_date) = "OPY" THEN "OFTALMOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "POP" THEN "OFTALMOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "MDO" THEN "ONCOLOGÍA MÉDICA"
        WHEN MAX_BY(specialization, services_created_date) = "OEL" THEN "ÓRBITA, PÁRPADOS Y VÍAS LAGRIMALES"
        WHEN MAX_BY(specialization, services_created_date) = "OTR" THEN "ORTOPEDIA Y TRAUMATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "POT" THEN "ORTOPEDIA Y TRAUMATOLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "PVF" THEN "SUELO PÉLVICO"
        WHEN MAX_BY(specialization, services_created_date) = "NCP" THEN "PRÁCTICAS NARRATIVAS Y COLABORATIVAS EN PSICOTERAPIA"
        WHEN MAX_BY(specialization, services_created_date) = "OTY" THEN "OTORRINOLARINGOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "PDS" THEN "PEDIATRÍA"
        WHEN MAX_BY(specialization, services_created_date) = "CLP" THEN "PSICOLOGÍA CLÍNICA"
        WHEN MAX_BY(specialization, services_created_date) = "PSY" THEN "PSIQUIATRÍA"
        WHEN MAX_BY(specialization, services_created_date) = "VSA" THEN "RADIOLOGÍA VASCULAR E INTERVENCIONISTA"
        WHEN MAX_BY(specialization, services_created_date) = "RON" THEN "RADIOONCOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "HJR" THEN "RECONSTRUCCIÓN DE CADERA"
        WHEN MAX_BY(specialization, services_created_date) = "JRA" THEN "REEMPLAZO ARTICULAR Y ARTROSCOPIA"
        WHEN MAX_BY(specialization, services_created_date) = "RHY" THEN "REUMATOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "KNT" THEN "TRASPLANTE RENAL"
        WHEN MAX_BY(specialization, services_created_date) = "URG" THEN "URGENCIOLOGÍA" 
        WHEN MAX_BY(specialization, services_created_date) = "URY" THEN "UROLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "GYU" THEN "UROLOGÍA GINECOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "ONU" THEN "UROLOGÍA ONCOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "PDU" THEN "UROLOGÍA PEDIÁTRICA"
        WHEN MAX_BY(specialization, services_created_date) = "INY" THEN "INFECTOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "CLY" THEN "COLOPROCTOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "AOS" THEN "AUDIOLOGÍA, OTONEUROLOGÍA Y TERAPIA DEL HABLA"
        WHEN MAX_BY(specialization, services_created_date) = "PNM" THEN "NEUMOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "AIM" THEN "ALERGOLOGÍA E INMUNOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "GAE" THEN "ENDOSCOPIA GASTROINTESTINAL"
        WHEN MAX_BY(specialization, services_created_date) = "CAP" THEN "PSIQUIATRÍA INFANTIL Y ADOLESCENTE"
        WHEN MAX_BY(specialization, services_created_date) = "PSI" THEN "PSICOLOGÍA"
        WHEN MAX_BY(specialization, services_created_date) = "ONT" THEN "NUTRICIÓN ONCOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "UCE" THEN "EMERGENCIAS CARDIOVASCULARES"
        WHEN MAX_BY(specialization, services_created_date) = "RHM" THEN "REHABILITACIÓN"
        WHEN MAX_BY(specialization, services_created_date) = "RBS" THEN "CIRUGÍA ROBÓTICA"
        WHEN MAX_BY(specialization, services_created_date) = "ORS" THEN "ORTOPEDIA"
        WHEN MAX_BY(specialization, services_created_date) = "OOG" THEN "GINECOLOGÍA ONCOLÓGICA"
        WHEN MAX_BY(specialization, services_created_date) = "NUT" THEN "NUTRICIÓN"
        WHEN MAX_BY(specialization, services_created_date) = "DGA" THEN "ENDOSCOPIA DIAGNÓSTICA Y TERAPÉUTICA"
        ELSE UPPER(MAX_BY(specialization, services_created_date))
      END AS consult_specialization,
      MAX_BY(services_process_state, services_created_date) AS services_state,
      COALESCE(MAX_BY(services_diagnosis_icd, services_created_date), MAX_BY(caseeventfinalletterdata_cie_codes, services_created_date), MAX_BY(disease_origin_diagnosis_icd, services_created_date), MAX_BY(evaluation_cie_keys, services_created_date)) AS icd,
      --caseeventfinalletterdata_cie_descriptions,
      COALESCE(MAX_BY(services_cpt, services_created_date), MAX_BY(caseeventfinalletterdata_cpt_codes, services_created_date)) AS cpt,
      --caseeventfinalletterdata_cpt_descriptions,
      COALESCE(MAX_BY(diseasecase_description, services_created_date), MAX_BY(diagnosis, services_created_date)) AS diagnosis,
      MAX_BY(services_diagnosis_description, services_created_date) AS services_diagnosis_description,
      --services_service_description,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT interrogation_system IGNORE NULLS), ', ') AS med_note_system,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT med_note_motive IGNORE NULLS), ', ') AS med_note_motive,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT services_specification IGNORE NULLS), ', ') AS services_specification,
      --suffering,

    FROM
      base
    GROUP BY
      member_id, service_id, healthplan_id
  ),
  service_payments AS (
    SELECT
      member_id, service_id, healthplan_id,

      MAX_BY(service_type, transaction_date) AS service_type,
      MAX_BY(age_at_subscription, transaction_date) AS age_at_subscription,
      MAX_BY(gender, transaction_date) AS gender,
      MAX_BY(vc_schema, transaction_date) AS vc_schema,
      MAX_BY(ipc_schema, transaction_date) AS ipc_schema,
      MAX_BY(vertical, transaction_date) AS vertical,
      MAX_BY(is_internal, transaction_date) AS is_internal,
      MAX_BY(is_collective, transaction_date) AS is_collective,
      MAX_BY(product_id, transaction_date) AS product_id,
      MAX_BY(business_group_size, transaction_date) AS business_group_size,
      MAX_BY(business_group, transaction_date) AS business_group,
      MAX_BY(business_name, transaction_date) AS business_name,
      --transaction_id,
      MIN(transaction_date) AS transaction_date_min,
      MAX(transaction_date) AS transaction_date_max,
      ARRAY_TO_STRING(ARRAY_AGG(transaction_type IGNORE NULLS), ', ') AS array_transaction_type,
      SUM(subtotal) AS subtotal,
      --subtotal_cost_to_reinsurance,
      --reinsurance_participates,
    FROM
      `sofia-data-305018.views_20240702.dashboards_ue_03_service_payments`
    GROUP BY
      member_id, service_id, healthplan_id
  ),
  providers_doctor AS (
    SELECT DISTINCT
      id,
      nickname, 
      user_id,
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
  providers_doctorfriend AS (
    SELECT DISTINCT
      id,
      user_id,
      name, 
      tier
    FROM
    `sofia-data-305018.backend_db_20240702.providers_doctorfriend`
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
  app_address_doctor AS (
    SELECT
      doctor_id,
      MAX_BY(address1, updated_at) AS address1,
      MAX_BY(address2, updated_at) AS address2,
      MAX_BY(neighborhood, updated_at) AS neighborhood,
      MAX_BY(city, updated_at) AS city,
      MAX_BY(zipcode, updated_at) AS zipcode,
      MAX_BY(coordinates, updated_at) AS coordinates
    FROM
      `sofia-data-305018.backend_db_20240702.sofia_app_address`
    WHERE
      deleted IS NULL
    GROUP BY 
      doctor_id
  ),
  app_address AS (
    SELECT
      id,
      doctor_id,
      MAX_BY(address1, updated_at) AS address1,
      MAX_BY(address2, updated_at) AS address2,
      MAX_BY(neighborhood, updated_at) AS neighborhood,
      MAX_BY(city, updated_at) AS city,
      MAX_BY(zipcode, updated_at) AS zipcode,
      MAX_BY(coordinates, updated_at) AS coordinates
    FROM
      `sofia-data-305018.backend_db_20240702.sofia_app_address`
    WHERE
      deleted IS NULL
    GROUP BY 
      id,
      doctor_id
  ),
  location_location AS (
    SELECT
      zipcode,
      MAX_BY(municipality, is_available_updated_at) AS municipality,
      MAX_BY(state_name, is_available_updated_at) AS state_name,
      MAX_BY(city, is_available_updated_at) AS city
    FROM
      `sofia-data-305018.backend_db_20240702.location_location`
    WHERE
      is_available 
    GROUP BY 
      zipcode
  ),
  dr_specializations AS (
    SELECT
      doctor_id,
      ARRAY_AGG(DISTINCT pdms.name IGNORE NULLS) AS specialization_arr
    FROM 
      `sofia-data-305018.backend_db_20240702.providers_doctorspecialistdetail` AS sp
    LEFT JOIN 
      `sofia-data-305018.backend_db_20240702.providers_medicalspecialization` AS pdms 
    ON 
      pdms.id = sp.medical_specialization_id
    GROUP BY 1
  ),
  billinginfo AS (
    SELECT DISTINCT
      user_id,
      address_id
    FROM
      `sofia-data-305018.backend_db_20240702.payments_billinginfo`
  ),
  healthplan_healthplan AS (
    SELECT
      h.id,
      h.relationship,
      hap.business_code,
      CASE
        WHEN h.relationship = 'SL' THEN 'Titular'
        WHEN h.relationship = 'WF' THEN 'Esposa'
        WHEN h.relationship = 'HS' THEN 'Esposo'
        WHEN h.relationship = 'DG' THEN 'Hija'
        WHEN h.relationship = 'SN' THEN 'Hijo'
        WHEN hap.business_code IS NOT NULL THEN 'Empleado'
        ELSE NULL -- Add ELSE NULL to handle other cases
      END AS relationship_beneficiary_with_owner,
      CASE 
        WHEN hap.business_code IS NOT NULL THEN 'Persona Moral'
        ELSE 'Persona Física'
      END AS owner_type,
    FROM
      `sofia-data-305018.backend_db_20240702.healthplan_healthplan` AS h
    LEFT JOIN 
      `sofia-data-305018.backend_db_20240702.subscriptions_healthplanapplicationitem` AS hapi 
    ON h.health_plan_application_item_id = hapi.id
    LEFT JOIN 
      `sofia-data-305018.backend_db_20240702.subscriptions_healthplanapplication` AS hap 
    ON hapi.application_id = hap.id
  ),
  payment_mode AS (
    SELECT
      id, 
      service_id, 
      state, 
      updated_at,
      CASE 
        WHEN cscp.payout_mode = 'DIRECT' THEN 'Pago directo'
		    WHEN cscp.payout_mode = 'DIRECT_HOSPITAL' THEN 'Pago directo a hospital'
        WHEN cscp.payout_mode = 'DIRECT_CONSULTING_ROOM' THEN 'Pago directo a consultorio'
        WHEN cscp.payout_mode = 'REIMBURSEMENT' THEN 'Reembolso'
        WHEN cscp.payout_mode = 'REIMBURSEMENT_CFDI_RECIPIENT_MEMBER' THEN 'Reembolso (CFDI Soci@)'
        WHEN cscp.payout_mode = 'COMPENSATION' THEN 'Compensación'
		    ELSE cscp.payout_mode
		  END as payout_mode,
      CASE 
        WHEN cspi.method = 'CARD' THEN 'Pago con tarjeta'
        WHEN cspi.method = 'IN_SITU' THEN 'Pago in situ'
      END AS payin_mode
    FROM
      `sofia-data-305018.backend_db_20240702.claims_servicecoverageruling` AS cscr
    LEFT JOIN(
			SELECT
				service_coverage_ruling_id,
				payout_mode
			FROM(
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY service_coverage_ruling_id ORDER BY created_at DESC) AS row_num
					FROM
            `sofia-data-305018.backend_db_20240702.claims_servicecoveragepayout`
					WHERE
						deleted IS NULL
			) AS AUX
			WHERE
				row_num = 1
		) AS cscp 
    ON cscp.service_coverage_ruling_id = cscr.id 
    LEFT JOIN(
			SELECT
				service_coverage_ruling_id,
				method
			FROM(
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY service_coverage_ruling_id ORDER BY created_at DESC) AS row_num
					FROM
            `sofia-data-305018.backend_db_20240702.claims_servicepayinrequest`
					WHERE
						deleted IS NULL
			) AS AUX
			WHERE
				row_num = 1
		) AS cspi 
    ON cspi.service_coverage_ruling_id = cscr.id
  ),
  zipcode_address AS (
    SELECT 
      postal_code,
      MAX_BY(longitude, admin_name1) AS longitude,
      MAX_BY(latitude, admin_name1) AS latitude,
      MAX_BY(admin_name1, admin_name1) AS admin_name1,
      MAX_BY(admin_name2, admin_name1) AS admin_name2,
    FROM
      `sofia-data-305018.mlr_optimization.zipcode_mx` 
    GROUP BY 
      postal_code
  )


SELECT DISTINCT
  # parte medica del servicio
  clean_base.service_id,
  clean_base.case_event_id,
  clean_base.disease_case_id,
  clean_base.related_service_id,
  COALESCE(clean_base.service_type, service_payments.service_type) AS service_type,
  COALESCE(st.descripcion, 'Otro servicio') AS service_type_from_description,
  clean_base.services_created_date,
  clean_base.services_state,
  clean_base.cpt,
  clean_base.icd,
  clean_base.diagnosis,
  clean_base.services_diagnosis_description,
  clean_base.med_note_system,
  clean_base.med_note_motive,
  clean_base.services_specification,
  caseevent_admission_source,
  caseevent_category,
  caseevent_class,

  
  # proveedor 
  providerbranchoffice.name AS provider_name,
  clean_base.provider_network,
  providers_provider.contract_category AS provider_contract_category,
  providercategory.name AS provider_category_name,
  providers_provider.website AS provider_website,
  providers_provider.state AS provider_state,
  providers_provider.notes AS provider_notes,
  app_address_provider.address1 AS provider_address1,
  app_address_provider.address2 AS provider_address2,
  app_address_provider.neighborhood AS provider_address_neighborhood,
  app_address_provider.city AS provider_address_city,
  app_address_provider.zipcode AS provider_address_zipcode,
  zipcode_mx_provider.longitude AS provider_longitude,
  zipcode_mx_provider.latitude AS provider_latitude,
  --zipcode_mx_provider.place_name AS provider_place_name,
  zipcode_mx_provider.admin_name1 AS provider_admin_name1,
  zipcode_mx_provider.admin_name2 AS provider_admin_name2,
  location_location_provider.municipality AS provider_location_municipality,
  location_location_provider.state_name AS provider_location_state,
  location_location_provider.city AS provider_location_city,
  
  
  # informacion del doctor
  clean_base.doctor_specialization,
  clean_base.consult_specialization,
  providers_doctorfriend.name AS doctorfriend_name,
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
  clean_base.doctor_specialization AS doctor_specialization_detail,
  ARRAY_TO_STRING(dr_specializations.specialization_arr, ', ') AS doctor_specializations,
  providers_specializationfare.name AS doctor_specializationfare_name,
  providers_medicalspecialization.name AS doctor_specialization_name,
  providers_medicalspecialization.description AS doctor_specialization_description,
  providers_medicalspecialization.type AS doctor_specialization_type,
  providers_medicalspecialization.is_primary_care AS doctor_specialization_is_primary_care,
  app_address_doctor.address1 AS doctor_address1,
  app_address_doctor.address2 AS doctor_address2,
  app_address_doctor.neighborhood AS doctor_neighborhood,
  app_address_doctor.city AS doctor_city,
  app_address_doctor.zipcode AS doctor_zipcode,
  zipcode_mx_doctor.longitude AS doctor_longitude,
  zipcode_mx_doctor.latitude AS doctor_latitude,
  --zipcode_mx_doctor.place_name AS doctor_place_name,
  zipcode_mx_doctor.admin_name1 AS doctor_admin_name1,
  zipcode_mx_doctor.admin_name2 AS doctor_admin_name2,
  location_location_doctor.municipality AS doctor_location_municipality,
  location_location_doctor.state_name AS doctor_location_state,
  location_location_doctor.city AS doctor_location_city,
  


  # personales del socio
  clean_base.member_id,
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
  app_address_member.address1 AS member_address1,
  app_address_member.address2 AS member_address2,
  app_address_member.neighborhood AS member_address_neighborhood,
  app_address_member.city AS member_address_city,
  app_address_member.zipcode AS member_address_zipcode,
  zipcode_mx_member.longitude AS member_longitude,
  zipcode_mx_member.latitude AS member_latitude,
  --zipcode_mx_member.place_name AS member_place_name,
  zipcode_mx_member.admin_name1 AS member_admin_name1,
  zipcode_mx_member.admin_name2 AS member_admin_name2,
  location_location_member.municipality AS member_location_municipality,
  location_location_member.state_name AS member_location_state,
  location_location_member.city AS member_location_city,


  # primas
  healthplan_healthplan.relationship_beneficiary_with_owner,
  healthplan_healthplan.owner_type,
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
  service_payments.transaction_date_min,
  service_payments.transaction_date_max,
  service_payments.array_transaction_type,
  payment_mode.payout_mode,
  payment_mode.payin_mode,
  service_payments.subtotal,
	
FROM
  service_payments --76,058,793
JOIN
  clean_base --76,056,633
USING(member_id, service_id, healthplan_id) 

LEFT JOIN 
  healthplan_healthplan --76,056,633
ON clean_base.healthplan_id = healthplan_healthplan.id

LEFT JOIN
  payment_mode --76,056,633
ON payment_mode.service_id = clean_base.service_id

LEFT JOIN 
  `sofia-data-305018.common_20240702.service_types` AS st --76,056,633
ON st.code = clean_base.services_service_type_value


LEFT JOIN 
  providers_doctor --76,056,633
ON providers_doctor.id = clean_base.provided_by_object_id AND clean_base.provided_by_content_type_id = 59
LEFT JOIN
  providers_specializationfare
ON providers_doctor.specialization_fare_id = providers_specializationfare.id -- tarifa de acuerdo a la especializacion (no hay para dr amigo)
LEFT JOIN
  providers_medicalspecialization
ON providers_medicalspecialization.id = providers_specializationfare.medical_specialization_id -- solo para dr de red
LEFT JOIN 
  dr_specializations
ON dr_specializations.doctor_id = providers_doctor.id


LEFT JOIN 
  providers_doctorfriend --76,056,633
ON providers_doctorfriend.id = clean_base.provided_by_object_id AND clean_base.provided_by_content_type_id = 58


LEFT JOIN
  providerbranchoffice --76,056,633
ON clean_base.provided_by_object_id = providerbranchoffice.id AND clean_base.provided_by_content_type_id = 57 
LEFT JOIN
  providers_provider -- Solo para los branch office (casa matriz)
ON providerbranchoffice.provider_id = providers_provider.id
LEFT JOIN 
  providercategory
ON providercategory.id = providers_provider.category_id


LEFT JOIN 
  app_user AS app_user_doctor --76,056,633
ON providers_doctor.user_id = app_user_doctor.id
LEFT JOIN 
  app_address_doctor
ON app_address_doctor.doctor_id = providers_doctor.id
LEFT JOIN 
  location_location AS location_location_doctor
ON location_location_doctor.zipcode = app_address_doctor.zipcode


LEFT JOIN 
  app_user AS app_user_provider --76,056,633
ON providerbranchoffice.admin_user_id = app_user_provider.id
LEFT JOIN 
  billinginfo
ON billinginfo.user_id = app_user_provider.id
LEFT JOIN 
  app_address AS app_address_provider
ON billinginfo.address_id = app_address_provider.id
LEFT JOIN 
  location_location AS location_location_provider
ON location_location_provider.zipcode = app_address_provider.zipcode


LEFT JOIN
  app_member --76,056,633
ON clean_base.member_id = app_member.id
LEFT JOIN 
  app_address AS app_address_member
ON app_member.residence_address_id = app_address_member.id
LEFT JOIN 
   location_location AS location_location_member
ON location_location_member.zipcode = app_address_member.zipcode

LEFT JOIN 
  zipcode_address AS zipcode_mx_provider
ON zipcode_mx_provider.postal_code = app_address_provider.zipcode

LEFT JOIN 
  zipcode_address AS zipcode_mx_doctor
ON zipcode_mx_doctor.postal_code = app_address_doctor.zipcode

LEFT JOIN 
  zipcode_address AS zipcode_mx_member
ON zipcode_mx_member.postal_code = app_address_member.zipcode


-- Chopo generico es un laboratorio donde tenemos convenio
  -- imagenes y lab
-- provider name es el principal y deberia ser en casos de lab y hospi
-- siniestro = caso
-- reclamacion = servicio
-- service_
-- relevantes: content_type_id (servicio, endoso), invoice_item: health_local_taxes (retencion de impuestos)
-- Alan se maneja
-- Se lee el XML de la factura para llenar los campos 
-- Hospitalizacion: Urgencias o admision hospitalaria
-- Ver tablas de preexistencias, socios pueden no tener servicios pero si tener la enfermedad



-- No tengo idea quien lo hizo, falta informacion de proveedor
-- Especialziacion de lo que se ha hecho
-- Muchos genericos y None
-- service payment tienen logicas distintas de servicio case y evento y no esta tan depurado
-- Documentar findings de calidad de datos, campos relevantes y fuentes
