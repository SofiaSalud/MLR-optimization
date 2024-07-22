--CREATE OR REPLACE TABLE `dashboards_views.dashboards_ue_03_service_payments` AS 
-- ACCURACY NO COMPROBADO. NO SE ASEGURA IGUALDAD ENTRE LA TABLA GENERADA EN POSTGRES Y BIGQUERY

WITH all_days_pre AS (
    SELECT
      GENERATE_DATE_ARRAY(
        '2020-11-24',
        common_20240702.tz_mex(CURRENT_TIMESTAMP()),
        INTERVAL 1 DAY
      ) AS ts_date
  ),
  all_days AS (
    SELECT
      ts_date,
      DATE_TRUNC(ts_date, MONTH) AS month
    FROM
      all_days_pre,
      UNNEST(ts_date) AS ts_date
  ),
  plan_data AS (
    SELECT
      healthplan_id,
      product_id,
      product_family,
      business_id,
      business_group_size,
      business_group,
      business_name,
      is_collective,
      is_internal,
      vertical,
      age_at_subscription,
      gender
    FROM (
      SELECT
        op.*,
        ROW_NUMBER() OVER (PARTITION BY op.healthplan_id ORDER BY op.amendment_id) AS rn
      FROM
        dashboards_views.dashboards_ue_01_premium AS op
    ) AS AUX
    WHERE
      rn = 1
  ),
  content_type_in_scope AS (
    SELECT
      id,
      model
    FROM
      backend_db_20240702.django_content_type
    WHERE
      app_label IN ('claims', 'pharmacy') -- VCs no importan, su costo sale de otra manera
  ),
  invoiceitems_in_scope AS (
    SELECT
      id,
      object_id,
      subtotal_cents,
      payment_flow
    FROM
      backend_db_20240702.invoice_invoiceitem
    WHERE
      content_type_id IN (
        SELECT
          id
        FROM
          content_type_in_scope
      )
      AND deleted IS NULL
  ),
  transactions_in_scope AS (
    SELECT
      id,
      invoice_id,
      transaction_type,
      confirmed_at
    FROM
      backend_db_20240702.payments_transaction
    WHERE
      transaction_type IN ('PY', 'DB', 'RF', 'FW', 'PR')
      AND payment_gateway_result = 'OK'
      AND deleted IS NULL
      AND invoice_id IS NOT NULL
  ),
  non_sofiamed_paid_services_pre1 AS (
    SELECT
      ss.health_plan_id AS healthplan_id,
      ss.member_id,
      ss.id AS service_id,
      ss.disease_case_id,
      DATE(common_20240702.tz_mex(ss.created_at)) AS service_created_at,
      COALESCE(st.legend, 'Other service') AS service_type,
      CAST(NULL AS STRING) AS vc_schema,
      CASE
        WHEN ss.service_type_value = 'IN_PERSON_CONSULT' THEN 'Fee per service'
      END AS ipc_schema,
      pt.id AS transaction_id,
      pt.transaction_type,
      common_20240702.tz_mex(pt.confirmed_at) AS transaction_date,
      ROUND(
        ii.subtotal_cents / 100.00,
        2
      ) * (
        CASE
          WHEN ii.payment_flow = 'OUT' THEN 1
          ELSE -1
        END
      ) * (
        CASE
          WHEN pt.transaction_type IN ('DB', 'PY') THEN 1
          ELSE -1
        END
      ) AS subtotal
    FROM
      invoiceitems_in_scope AS ii
      LEFT JOIN backend_db_20240702.services_service AS ss 
        ON ii.object_id = ss.id
      LEFT JOIN backend_db_20240702.providers_doctor AS d 
        ON ss.provided_by_object_id = d.id AND ss.provided_by_content_type_id = 59
      LEFT JOIN backend_db_20240702.providers_providerbranchoffice AS o 
        ON d.provider_branch_office_id = o.id
      LEFT JOIN backend_db_20240702.providers_provider AS p 
        ON p.id = o.provider_id
      LEFT JOIN backend_db_20240702.invoice_invoiceitem_invoice AS iii 
        ON iii.invoiceitem_id = ii.id
      LEFT JOIN transactions_in_scope AS pt 
        ON pt.invoice_id = iii.invoice_id
      LEFT JOIN common_20240702.service_types AS st 
        ON st.code = ss.service_type_value
    WHERE
      ss.deleted IS NULL
      AND ss.service_type_value != 'ON_DEMAND_CONSULT'
      AND NOT (
        ss.service_type_value = 'IN_PERSON_CONSULT_BY_PROVIDER'
        AND p.name = 'SofiaMed'
      )
  ),
  non_sofiamed_paid_services_pre2 AS (
    SELECT
      *
    FROM
      non_sofiamed_paid_services_pre1
    WHERE
      transaction_date IS NOT NULL
  ),
  non_sofiamed_paid_services AS (
    SELECT
      ss.healthplan_id,
      member_id,
      is_internal,
      vertical,
      is_collective,
      product_id,
      product_family,
      business_id,
      business_group_size,
      business_group,
      business_name,
      age_at_subscription,
      gender,
      service_id,
      disease_case_id,
      service_created_at,
      service_type,
      vc_schema,
      ipc_schema,
      transaction_id,
      transaction_type,
      transaction_date,
      subtotal
    FROM
      non_sofiamed_paid_services_pre2 AS ss
      LEFT JOIN plan_data AS h 
        USING(healthplan_id)
  ),
  vc_payments_backend AS (
    SELECT
      CASE
        WHEN EXTRACT(DAY FROM common_20240702.tz_mex(pt.confirmed_at)) <= 10
          THEN DATE_TRUNC(
            DATE_SUB(common_20240702.tz_mex(pt.confirmed_at), INTERVAL 1 MONTH),
            MONTH
          )
        ELSE DATE_TRUNC(common_20240702.tz_mex(pt.confirmed_at), MONTH)
      END AS month,
      ROUND(SUM(subtotal_cents / 100.00), 2) AS subtotal
    FROM
      backend_db_20240702.invoice_invoiceitem AS ii
      LEFT JOIN backend_db_20240702.invoice_invoiceitem_invoice AS iii ON iii.invoiceitem_id = ii.id
      LEFT JOIN backend_db_20240702.payments_transaction AS pt ON pt.transaction_type = 'PY'
      AND pt.payment_gateway_result = 'OK'
      AND pt.invoice_id = iii.invoice_id
    WHERE
      ii.content_type_id IN (
        SELECT
          id
        FROM
          backend_db_20240702.django_content_type
        WHERE
          model = 'ondemandconsulttimelog'
      )
      AND pt.id IS NOT NULL
      AND ii.deleted IS NULL
      AND pt.deleted IS NULL
    GROUP BY
      month
  ),
  vc_payments_bbva AS (
    SELECT
      month, 
      subtotal
    FROM(
      SELECT
        DATE('2020-11-01') AS month,
        ROUND(2756.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2020-11-01') AS month,
        ROUND(1560.00 / 0.90, 2) AS subtotal -- por que son distintos valores en misma fecha?
      UNION ALL
      SELECT
        DATE('2020-12-01') AS month,
        ROUND(260.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2020-12-01') AS month,
        ROUND(14196.00 / 0.90, 2) AS subtotal -- por que son distintos valores en misma fecha?
      UNION ALL
      SELECT
        DATE('2021-01-01') AS month,
        ROUND(2340.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-02-01') AS month,
        ROUND(6812.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-03-01') AS month,
        ROUND(7176.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-04-01') AS month,
        ROUND(5460.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-05-01') AS month,
        ROUND(3120.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-10-01') AS month,
        ROUND(5850.00 / 0.90, 2) AS subtotal
      UNION ALL
      SELECT
        DATE('2021-11-01') AS month,
        47740 AS subtotal
      UNION ALL
      SELECT
        DATE('2021-12-01') AS month,
        56234 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-01-01') AS month,
        45314 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-02-01') AS month,
        44766 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-03-01') AS month,
        38058 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-04-01') AS month,
        49226 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-05-01') AS month,
        40144 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-06-01') AS month,
        38688 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-07-01') AS month,
        79976 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-08-01') AS month,
        92584 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-09-01') AS month,
        91208 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-10-01') AS month,
        125182 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-11-01') AS month,
        136476 AS subtotal
      UNION ALL
      SELECT
        DATE('2022-12-01') AS month,
        145094 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-01-01') AS month,
        178678 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-02-01') AS month,
        151130 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-03-01') AS month,
        199992 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-04-01') AS month,
        237634 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-05-01') AS month,
        196056 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-06-01') AS month,
        273796 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-07-01') AS month,
        337856 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-08-01') AS month,
        ROUND(
          254755 / 4.0 * 4.34
        ) AS subtotal
      UNION ALL
      SELECT
        DATE('2023-09-01') AS month,
        296472 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-10-01') AS month,
        258816 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-11-01') AS month,
        234180 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-12-01') AS month,
        202070 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-01-01') AS month,
        222260 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-02-01') AS month,
        259546 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-03-01') AS month,
        323972 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-04-01') AS month,
        248256 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-05-01') AS month,
        228502 AS subtotal
    ) AS t
  ),
  ipcs_sofiamed_costs_hours AS (
    SELECT
      month,
      doctor_id,
      doctor_name,
      subtotal
    FROM(
      SELECT
        DATE('2023-04-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-05-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-05-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        12720.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-06-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-06-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-07-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-07-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-08-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-08-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-09-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-09-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-10-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-10-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-11-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-11-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-12-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2023-12-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-01-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-01-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-02-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-02-01') AS month,
        863 AS doctor_id,
        'Paulina Guadalupe Reyes' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-02-01') AS month,
        155 AS doctor_id,
        'Virginia Montserrat Martínez Muñoz' AS doctor_name,
        1908.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-03-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-03-01') AS month,
        155 AS doctor_id,
        'Virginia Montserrat Martínez Muñoz' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-04-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-04-01') AS month,
        155 AS doctor_id,
        'Virginia Montserrat Martínez Muñoz' AS doctor_name,
        16960.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-05-01') AS month,
        497 AS doctor_id,
        'Alejandra Alicia Barrientos' AS doctor_name,
        19080.00 AS subtotal
      UNION ALL
      SELECT
        DATE('2024-05-01') AS month,
        155 AS doctor_id,
        'Virginia Montserrat Martínez Muñoz' AS doctor_name,
        19083.00 AS subtotal
    ) AS t
  ),
  vc_costs_hours AS (
    SELECT
      month,
      'Fee per hour' AS vc_schema,
      SUM(subtotal) AS subtotal
    FROM(
      SELECT
        *
      FROM
        vc_payments_backend
      UNION ALL
      SELECT
        *
      FROM
        vc_payments_bbva
    ) AS aux
    GROUP BY
      month,
      vc_schema
    ),
  vc_costs_payroll AS (
        SELECT
            month,
            'Payroll based' AS vc_schema,
            ROUND(SUM(daily_salary), 2) AS subtotal
        FROM(
            SELECT
                ts_date,
                month,
                (SELECT COUNT(doc.doctor_id) FROM common_20240702.doctores_cabecera AS doc WHERE doc.start_date <= ts_date AND COALESCE(doc.end_date, DATE('2100-01-01')) >= ts_date AND doc.type = 'Dr de Cabecera') AS cuenta_doctores,
                (SELECT SUM(doc.salary) FROM common_20240702.doctores_cabecera AS doc WHERE doc.start_date <= ts_date AND COALESCE(doc.end_date, DATE('2100-01-01')) >= ts_date AND doc.type = 'Dr de Cabecera') 
                / 
                EXTRACT(DAY FROM (DATE_TRUNC(ts_date, MONTH) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) 
                AS daily_salary
            FROM
                all_days AS dt
            WHERE
                dt.ts_date >= '2020-11-24'
        ) AS aux
        GROUP BY
            month
    ),
    vc_costs AS (
        SELECT
            *
        FROM
            vc_costs_hours
        UNION ALL
        SELECT
            *
        FROM
            vc_costs_payroll
    ),
    vc_base AS (
        SELECT
            vc_schema,
            healthplan_id,
            member_id,
            service_id,
            disease_case_id,
            service_created_at
        FROM (
            SELECT
                ss.health_plan_id AS healthplan_id,
                ss.member_id,
                ss.id AS service_id,
                ss.disease_case_id,
                CAST(common_20240702.tz_mex(ss.created_at) AS DATE) AS service_created_at,
                ROUND(
                    TIMESTAMP_DIFF(LEAD(ss.created_at) OVER (PARTITION BY ss.member_id ORDER BY ss.created_at ASC), ss.created_at, MINUTE),
                    2
                ) AS minutes_to_next_vc,
                CASE
                    WHEN dc.doctor_id IS NOT NULL
                    THEN 'Payroll based'
                    ELSE 'Fee per hour'
                END AS vc_schema
            FROM
                `backend_db_20240702.services_service` AS ss
                LEFT JOIN `backend_db_20240702.consult_ondemandconsult` AS con ON ss.id = con.consult_ptr_id
                LEFT JOIN `common_20240702.doctores_cabecera` AS dc ON dc.doctor_id = ss.provided_by_object_id
                AND common_20240702.tz_mex(ss.created_at) >= dc.start_date
                AND common_20240702.tz_mex(ss.created_at) <= COALESCE(
                    dc.end_date,
                    DATE('2100-01-01')
                )
                AND dc.type = 'Dr de Cabecera'
            WHERE
                ss.service_type_value = 'ON_DEMAND_CONSULT'
                AND ss.process_state != 'CANCELLED'
                AND con.state NOT IN ('CA', 'HN')
                AND ss.deleted IS NULL
                AND NOT REGEXP_CONTAINS(LOWER(con.reason), 'prueba|test')
        ) AS before_removing_dups
        WHERE
            minutes_to_next_vc IS NULL
            OR minutes_to_next_vc > 60
    ),
vc_count AS (
    SELECT
        DATE_TRUNC(service_created_at, MONTH) AS month,
        vc_schema,
        COUNT(DISTINCT service_id) AS count_service
    FROM
        vc_base
    GROUP BY
        1,
        2
),
vc_unit_costs_excl_last_month AS (
    SELECT
        vc_count_1.month,
        vc_count_1.vc_schema,
        CASE
            WHEN vc_count_1.month = DATE_TRUNC(common_20240702.tz_mex(CURRENT_TIMESTAMP()), MONTH)
            THEN NULL
            ELSE vc_costs_1.subtotal
        END AS monthly_total_cost,
        vc_count_1.count_service AS VCs,
        vc_costs_1.subtotal / vc_count_1.count_service AS unit_cost
    FROM
        vc_count AS vc_count_1
        LEFT JOIN vc_costs AS vc_costs_1 ON vc_count_1.month = vc_costs_1.month
        AND vc_count_1.vc_schema = vc_costs_1.vc_schema
    WHERE
        vc_count_1.month < DATE_TRUNC(common_20240702.tz_mex(CURRENT_TIMESTAMP()), MONTH)
),
vc_costs_last_month AS (
    SELECT
        vc_schema,
        unit_cost
    FROM
        vc_unit_costs_excl_last_month
    WHERE
        month = DATE_TRUNC(
            DATE_SUB(DATE_TRUNC(common_20240702.tz_mex(CURRENT_TIMESTAMP()), MONTH), INTERVAL 1 MONTH),
            MONTH
        )
),
current_month_unit_costs_est AS (
    SELECT
        vc_count.month,
        vc_count.vc_schema,
        vc_count.count_service * vc_costs_last_month.unit_cost AS monthly_total_cost,
        vc_count.count_service AS VCs,
        vc_costs_last_month.unit_cost
    FROM
        vc_count
        LEFT JOIN vc_costs_last_month USING(vc_schema)
    WHERE
        month = DATE_TRUNC(common_20240702.tz_mex(CURRENT_TIMESTAMP()), MONTH)
),
vc_unit_costs AS (
    SELECT
        *
    FROM
        vc_unit_costs_excl_last_month
    UNION ALL (
        SELECT
            *
        FROM
            current_month_unit_costs_est
    )
),
vc_base_with_cost AS (
    SELECT
        vc_base.*,
        vc_unit_costs.unit_cost AS subtotal
    FROM
        vc_base
        LEFT JOIN vc_unit_costs ON DATE_TRUNC(vc_base.service_created_at, MONTH) = vc_unit_costs.month
        AND vc_base.vc_schema = vc_unit_costs.vc_schema
),
vc_paid_services AS (
    SELECT
        vc_base_with_cost.healthplan_id,
        vc_base_with_cost.member_id,
        h.is_internal,
        h.vertical,
        h.is_collective,
        h.product_id,
        h.product_family,
        h.business_id,
        h.business_group_size,
        h.business_group,
        h.business_name,
        h.age_at_subscription,
        h.gender,
        vc_base_with_cost.service_id,
        vc_base_with_cost.disease_case_id,
        vc_base_with_cost.service_created_at,
        'Videoconsult' AS service_type,
        vc_base_with_cost.vc_schema,
        CAST(NULL AS STRING) AS ipc_schema,
        CAST(NULL AS INT64) AS transaction_id,
        'PY' AS transaction_type,
        vc_base_with_cost.service_created_at AS transaction_date,
        vc_base_with_cost.subtotal
    FROM
        vc_base_with_cost
        LEFT JOIN plan_data AS h USING(healthplan_id)
),
ipcs_sofiamed_all_schemas_base AS (
    SELECT
        ss.health_plan_id AS healthplan_id,
        ss.member_id,
        ss.id AS service_id,
        ss.disease_case_id,
        CAST(common_20240702.tz_mex(ss.created_at) AS DATE) AS service_created_at,
        CASE
            WHEN dc.doctor_id IS NULL
            THEN 'Fee per hour'
            ELSE 'Payroll based'
        END AS ipc_schema,
        d.id AS doctor_id
    FROM
        `backend_db_20240702.services_service` AS ss
        LEFT JOIN `backend_db_20240702.claims_servicecoverageruling` AS r ON r.service_id = ss.id
        LEFT JOIN `backend_db_20240702.providers_doctor` AS d ON ss.provided_by_object_id = d.id
        AND ss.provided_by_content_type_id = 59
        LEFT JOIN `backend_db_20240702.providers_providerbranchoffice` AS o ON d.provider_branch_office_id = o.id
        LEFT JOIN `backend_db_20240702.providers_provider` AS p ON p.id = o.provider_id
        LEFT JOIN `common_20240702.doctores_cabecera` AS dc ON dc.doctor_id = d.id
        AND common_20240702.tz_mex(ss.created_at) >= dc.start_date
        AND common_20240702.tz_mex(ss.created_at) <= COALESCE(
            dc.end_date,
            DATE('2100-01-01')
        )
        AND dc.type != 'Dr de Cabecera'
    WHERE
        ss.service_type_value = 'IN_PERSON_CONSULT_BY_PROVIDER'
        AND ss.process_state != 'CANCELLED'
        AND ss.deleted IS NULL
        AND r.state != 'CN'
        AND p.name = 'SofiaMed'
),
ipcs_sofiamed_hourly_schema_base AS (
    SELECT
        healthplan_id,
        member_id,
        service_id,
        disease_case_id,
        service_created_at,
        doctor_id
    FROM
        ipcs_sofiamed_all_schemas_base
    WHERE
        ipc_schema = 'Fee per hour'
),
ipcs_sofiamed_hourly_schema_count AS (
    SELECT
        DATE_TRUNC(service_created_at, MONTH) AS month,
        doctor_id,
        COUNT(DISTINCT service_id) AS count_service
    FROM
        ipcs_sofiamed_hourly_schema_base
    GROUP BY
        1,
        2
    ORDER BY
        1,
        2
),
ipcs_sofiamed_hourly_schema_unit_costs AS (
    SELECT
        *,
        CASE
            WHEN month < DATE_TRUNC(common_20240702.tz_mex(CURRENT_TIMESTAMP()), MONTH)
            AND subtotal IS NOT NULL
            THEN CAST(subtotal AS BIGNUMERIC) / count_service
            ELSE LAG(CAST(subtotal AS BIGNUMERIC) / count_service) OVER (PARTITION BY doctor_id ORDER BY
                month
            )
        END AS unit_cost
    FROM
        ipcs_sofiamed_hourly_schema_count
        LEFT JOIN ipcs_sofiamed_costs_hours USING(month, doctor_id)
    ORDER BY
        1,
        2
),
ipcs_sofiamed_hourly_schema_base_with_cost AS (
    SELECT
        ipcs_sofiamed_hourly_schema_base.*,
        ROUND(
            ipcs_sofiamed_hourly_schema_unit_costs.unit_cost - ROUND(p.copay_in_person_consult_cents / 100.00 / 1.16, 2), -- por que 1.16
            2
        ) AS subtotal
    FROM
        ipcs_sofiamed_hourly_schema_base
        LEFT JOIN ipcs_sofiamed_hourly_schema_unit_costs ON DATE_TRUNC(
            ipcs_sofiamed_hourly_schema_base.service_created_at,
            MONTH
        ) = ipcs_sofiamed_hourly_schema_unit_costs.month
        AND ipcs_sofiamed_hourly_schema_base.doctor_id = ipcs_sofiamed_hourly_schema_unit_costs.doctor_id
        LEFT JOIN `backend_db_20240702.healthplan_healthplan` AS h ON h.id = ipcs_sofiamed_hourly_schema_base.healthplan_id
        LEFT JOIN `backend_db_20240702.subscriptions_product` AS p ON h.product_id = p.id
),
ipcs_sofiamed_hourly_schema_paid_services AS (
    SELECT
        ipcs_sofiamed_hourly_schema_base_with_cost.healthplan_id,
        ipcs_sofiamed_hourly_schema_base_with_cost.member_id,
        h.is_internal,
        h.vertical,
        h.is_collective,
        h.product_id,
        h.product_family,
        h.business_id,
        h.business_group_size,
        h.business_group,
        h.business_name,
        h.age_at_subscription,
        h.gender,
        ipcs_sofiamed_hourly_schema_base_with_cost.service_id,
        ipcs_sofiamed_hourly_schema_base_with_cost.disease_case_id,
        ipcs_sofiamed_hourly_schema_base_with_cost.service_created_at,
        'In person consult by provider' AS service_type,
        CAST(NULL AS STRING) AS vc_schema,
        'Fee per hour' AS ipc_schema,
        CAST(NULL AS INT64) AS transaction_id,
        'PY' AS transaction_type,
        ipcs_sofiamed_hourly_schema_base_with_cost.service_created_at AS transaction_date,
        ipcs_sofiamed_hourly_schema_base_with_cost.subtotal
    FROM
        ipcs_sofiamed_hourly_schema_base_with_cost
        LEFT JOIN plan_data AS h USING(healthplan_id)
),
distinct_ipc_payroll_doctors AS (
    SELECT
        doctor_id,
        start_date,
        end_date,
        salary
    FROM
        `common_20240702.doctores_cabecera` AS doctores_cabecera
    WHERE
        doctores_cabecera.type != 'Dr de Cabecera'
),
all_days_ipc_payroll_doctors_combs AS (
    SELECT
        a.*,
        d.doctor_id,
        d.salary / EXTRACT(
            DAY
            FROM
                DATE_SUB(DATE_ADD(DATE_TRUNC(ts_date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)
        ) AS daily_salary
    FROM
        all_days AS a
        LEFT JOIN distinct_ipc_payroll_doctors AS d ON d.start_date <= a.ts_date
        AND COALESCE(
            d.end_date,
            CAST('2100-01-01' AS DATE)
        ) >= a.ts_date
    WHERE
        d.doctor_id IS NOT NULL
),
ipcs_sofiamed_costs_payroll AS (
    SELECT
        month,
        doctor_id,
        'Payroll based' AS vc_schema,
        ROUND(SUM(daily_salary), 2) AS subtotal
    FROM
        all_days_ipc_payroll_doctors_combs
    GROUP BY
        1,
        2,
        3
    ORDER BY
        1,
        2
),



ipcs_sofiamed_payroll_schema_base AS (
        SELECT
            healthplan_id,
            member_id,
            service_id,
            disease_case_id,
            service_created_at,
            doctor_id
        FROM
            ipcs_sofiamed_all_schemas_base
        WHERE ipc_schema = 'Payroll based'
    ),

    ipcs_sofiamed_payroll_schema_count AS (
        SELECT
            DATE_TRUNC(service_created_at, MONTH) AS month,
            doctor_id,
            COUNT(DISTINCT service_id) AS count
        FROM
            ipcs_sofiamed_payroll_schema_base
        GROUP BY 1, 2
        ORDER BY 1, 2
    ),

    ipcs_sofiamed_payroll_schema_unit_costs AS (
        SELECT
            *,
            CASE
                WHEN month < DATE_TRUNC(DATETIME(CURRENT_TIMESTAMP(), 'America/Mexico_City'), MONTH) AND subtotal IS NOT NULL THEN subtotal / count
                ELSE COALESCE(LAG(subtotal / count) OVER (PARTITION BY doctor_id ORDER BY month), subtotal / count)
            END AS unit_cost
        FROM
            ipcs_sofiamed_payroll_schema_count
        LEFT JOIN
            ipcs_sofiamed_costs_payroll USING (month, doctor_id)
    ),

    ipcs_sofiamed_payroll_schema_base_with_cost AS (
        SELECT
            ipcs_sofiamed_payroll_schema_base.*,
            ROUND(unit_cost - ROUND(p.copay_in_person_consult_cents / 100.00 / 1.16, 2), 2) AS subtotal
        FROM
            ipcs_sofiamed_payroll_schema_base
        LEFT JOIN
            ipcs_sofiamed_payroll_schema_unit_costs
        ON
            DATE_TRUNC(ipcs_sofiamed_payroll_schema_base.service_created_at, MONTH) = ipcs_sofiamed_payroll_schema_unit_costs.month
        LEFT JOIN
            backend_db_20240702.healthplan_healthplan AS h ON h.id = ipcs_sofiamed_payroll_schema_base.healthplan_id
        LEFT JOIN
            backend_db_20240702.subscriptions_product AS p ON h.product_id = p.id
    ),

    ipcs_sofiamed_payroll_schema_paid_services AS (
        SELECT
            healthplan_id,
            member_id,
            is_internal,
            vertical,
            is_collective,
            product_id,
            product_family,
            business_id,
            business_group_size,
            business_group,
            business_name,
            age_at_subscription,
            gender,
            service_id,
            disease_case_id,
            service_created_at,
            'In person consult by provider' AS service_type,
            CAST(NULL AS STRING) AS vc_schema,
            'Payroll based' AS ipc_schema,
            CAST(NULL AS INT64) AS transaction_id,
            'PY' AS transaction_type,
            service_created_at AS transaction_date,
            subtotal
        FROM
            ipcs_sofiamed_payroll_schema_base_with_cost
        LEFT JOIN
            plan_data AS h USING (healthplan_id)
    ),

    before_chocolate_cost AS (
        SELECT *
        FROM (
            SELECT * FROM vc_paid_services
            UNION ALL
            SELECT * FROM non_sofiamed_paid_services
            UNION ALL
            SELECT * FROM ipcs_sofiamed_hourly_schema_paid_services
            UNION ALL
            SELECT * FROM ipcs_sofiamed_payroll_schema_paid_services
        ) AS U
    ),

    chocolate_cost_vcs_pre AS (
        SELECT
            service_id,
            service_type,
            subtotal
        FROM (
            SELECT
                op.service_id,
                op.service_created_at,
                op.service_type,
                ROUND((py.covered_subtotal_cents + py.coinsured_subtotal_cents) / 100.00, 2) AS subtotal,
                ROW_NUMBER() OVER (PARTITION BY op.service_id ORDER BY py.created_at DESC) AS row_num
            FROM
                before_chocolate_cost AS op
            LEFT JOIN
                backend_db_20240702.claims_servicecoverageruling AS r ON r.service_id = op.service_id
            LEFT JOIN
                backend_db_20240702.claims_servicecoveragepayout AS py ON py.service_coverage_ruling_id = r.id
            WHERE op.service_type = 'Videoconsult' AND py.deleted IS NULL
            ORDER BY 1, 3 DESC
        ) AS AUX
        WHERE row_num = 1
        ORDER BY
            service_type, service_created_at
    ),

    breakpoints_pre AS (
        SELECT
            subtotal,
            MIN(service_id) AS first_service_id
        FROM
            chocolate_cost_vcs_pre
        WHERE subtotal IS NOT NULL
        GROUP BY 1
        ORDER BY 2
    ),

    breakpoints AS (
        SELECT
            subtotal,
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY first_service_id) = 1 THEN 1
                ELSE first_service_id
            END AS first_service_id,
            COALESCE(LEAD(first_service_id) OVER (ORDER BY first_service_id) - 1, (SELECT MAX(service_id) FROM chocolate_cost_vcs_pre)) AS last_service_id
        FROM
            breakpoints_pre
        ORDER BY
            first_service_id
    ),

    chocolate_cost_vcs AS (
        SELECT
            b.service_id,
            bp.subtotal
        FROM
            chocolate_cost_vcs_pre AS b
        LEFT JOIN
            breakpoints AS bp ON b.service_id <= bp.last_service_id AND b.service_id >= bp.first_service_id
        ORDER BY
            b.service_id
    ),

    chocolate_cost_ipcs_pre AS (
        SELECT
            op.service_id,
            ss.provided_by_object_id,
            ROUND(py.covered_subtotal_cents / 100.00, 2) AS payout_subtotal,
            LAG(ROUND(py.covered_subtotal_cents / 100.00, 2)) OVER (PARTITION BY CAST(provided_by_object_id AS STRING) ORDER BY op.service_id) AS previous_payout_subtotal,
            LEAD(ROUND(py.covered_subtotal_cents / 100.00, 2)) OVER (PARTITION BY CAST(provided_by_object_id AS STRING) ORDER BY op.service_id) AS next_payout_subtotal,
            ROUND(fare.amount_cents / 100.00, 2) AS current_fare,
            ROUND(p.copay_in_person_consult_cents / 100.00 / 1.16, 2) AS copay
        FROM
            before_chocolate_cost AS op
        LEFT JOIN
            backend_db_20240702.services_service AS ss ON op.service_id = ss.id
        LEFT JOIN
            backend_db_20240702.providers_doctor AS d ON ss.provided_by_object_id = d.id
        LEFT JOIN
            backend_db_20240702.providers_specializationfare AS fare ON d.specialization_fare_id = fare.id
        LEFT JOIN
            backend_db_20240702.claims_servicecoverageruling AS r ON r.service_id = op.service_id
        LEFT JOIN
            backend_db_20240702.claims_servicecoveragepayout AS py ON py.service_coverage_ruling_id = r.id
        LEFT JOIN
            backend_db_20240702.healthplan_healthplan AS h ON op.healthplan_id = h.id
        LEFT JOIN
            backend_db_20240702.subscriptions_product AS p ON h.product_id = p.id
        WHERE op.service_type = 'In person consult by provider' AND ipc_schema != 'Fee per service' AND py.deleted IS NULL
        ORDER BY 1
    ),

    chocolate_cost_ipcs AS (
        SELECT
            service_id,
            COALESCE(COALESCE(COALESCE(payout_subtotal, previous_payout_subtotal), next_payout_subtotal), current_fare - copay) AS subtotal
        FROM
            chocolate_cost_ipcs_pre
    ),

    chocolate_costs AS (
        SELECT * FROM chocolate_cost_vcs
        UNION ALL
        SELECT * FROM chocolate_cost_ipcs
        ORDER BY 1
    ),

    main AS (
        SELECT
            before_chocolate_cost.healthplan_id,
            before_chocolate_cost.member_id,
            before_chocolate_cost.is_internal,
            before_chocolate_cost.vertical,
            before_chocolate_cost.is_collective,
            before_chocolate_cost.product_id,
            before_chocolate_cost.product_family,
            before_chocolate_cost.business_id,
            before_chocolate_cost.business_group_size,
            before_chocolate_cost.business_group,
            before_chocolate_cost.business_name,
            before_chocolate_cost.age_at_subscription,
            before_chocolate_cost.gender,
            before_chocolate_cost.service_id,
            before_chocolate_cost.disease_case_id,
            before_chocolate_cost.service_created_at,
            before_chocolate_cost.service_type,
            before_chocolate_cost.vc_schema,
            CASE
                WHEN before_chocolate_cost.service_type IN ('In person consult', 'In person consult by provider') THEN COALESCE(before_chocolate_cost.ipc_schema, 'Fee per service')
                ELSE NULL  -- Add an ELSE clause to handle other cases or leave as NULL
            END AS ipc_schema,
            before_chocolate_cost.transaction_id,
            before_chocolate_cost.transaction_type,
            before_chocolate_cost.transaction_date,
            before_chocolate_cost.subtotal,
            chocolate_costs.subtotal AS subtotal_cost_to_reinsurance,
            CASE
                WHEN before_chocolate_cost.product_family = 'mini' OR ncs.service_id IS NOT NULL OR nch.healthplan_uuid IS NOT NULL OR (before_chocolate_cost.service_type = 'Videoconsult' AND before_chocolate_cost.product_id < 40) THEN FALSE
                ELSE TRUE
            END AS reinsurance_participates
        FROM
            before_chocolate_cost
        LEFT JOIN
            chocolate_costs USING (service_id)
        LEFT JOIN
            common_20240702.non_ceded_services AS ncs USING (service_id)
        LEFT JOIN
            backend_db_20240702.healthplan_healthplan AS h ON before_chocolate_cost.healthplan_id = h.id
        LEFT JOIN
            common_20240702.non_ceded_healthplans AS nch ON h.uuid = nch.healthplan_uuid
    )

SELECT 
    *
FROM 
    main
