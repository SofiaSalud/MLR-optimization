--CREATE OR REPLACE TABLE dashboards_views.dashboards_ue_04_mlr_by_vertical AS
-- ACCURACY NO COMPROBADO. NO SE ASEGURA IGUALDAD ENTRE LA TABLA GENERADA EN POSTGRES Y BIGQUERY

WITH date_cutoffs AS (
    SELECT
        DATE_SUB(DATE_ADD(DATE('2020-11-01'), INTERVAL CAST(num AS INT64) MONTH), INTERVAL 1 DAY) AS date_date
    FROM
        UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), DATE('2020-11-01'), MONTH))) AS num
),

max_date AS (
    SELECT MAX(date_date) AS max_cutoff FROM date_cutoffs
),

hp_filters AS (
    SELECT DISTINCT healthplan_id
    FROM dashboards_views.dashboards_ue_01_premium
    WHERE is_internal = FALSE
),

plan_groups AS (
    SELECT DISTINCT
        healthplan_id,
        CASE
            WHEN vertical = 'B2C' THEN 'B2C'
            ELSE CONCAT(vertical, ' (', business_group_size, ')')
        END AS hp_class
    FROM dashboards_views.dashboards_ue_01_premium
    LEFT JOIN hp_filters USING(healthplan_id) -- Se estan añadiendo todos al hacer un left a toda la tabla
    WHERE hp_filters.healthplan_id IS NOT NULL
),

service_payments_in_scope_pre AS (
    SELECT dashboards_ue_03_service_payments.*
    FROM dashboards_views.dashboards_ue_03_service_payments
    LEFT JOIN hp_filters USING (healthplan_id)
    LEFT JOIN max_date ON TRUE -- Puede ser agregado como columna sin hacer un left join que tampoco tiene sentido
    WHERE hp_filters.healthplan_id IS NOT NULL
      AND transaction_date <= max_cutoff
      AND service_created_at <= max_cutoff
),

service_groups AS (
    SELECT DISTINCT service_id, 'All services' AS ss_class
    FROM service_payments_in_scope_pre
),

service_payments_in_scope AS (
    SELECT
        op.*,
        plan_groups.hp_class,
        service_groups.ss_class
    FROM service_payments_in_scope_pre AS op
    LEFT JOIN plan_groups USING (healthplan_id)
    LEFT JOIN service_groups USING (service_id) -- este no parece ser necesario
),

date_hp_combinations AS (
    SELECT *
    FROM date_cutoffs AS d
    CROSS JOIN (SELECT DISTINCT hp_class FROM plan_groups) AS aux1
),

accrued_collected_premium_by_hp_class_and_date AS (
    SELECT
        d.*,
        (
            SELECT SUM(premium_collected_and_accrued_to_date) -- Que es esto? 
            FROM (
                SELECT
                    amendment_id,
                    installment_number,
                    COALESCE(premium_collected_to_date, 0.00) *
                    (
                        GREATEST(
                            0,
                            CASE
                                WHEN DATE(installment_valid_through) <= LEAST(amendment_cancelation_date, DATE(installment_valid_through), d.date_date, common_20240702.tz_mex(CURRENT_TIMESTAMP()), plan_cancelation_date) THEN 1
                                WHEN DATE(installment_valid_since) > d.date_date THEN 0
                                ELSE (
                                    DATE_DIFF(LEAST(amendment_cancelation_date, DATE(installment_valid_through), d.date_date, common_20240702.tz_mex(CURRENT_TIMESTAMP()), plan_cancelation_date), DATE(installment_valid_since), DAY) + 1
                                ) / (
                                    DATE_DIFF(DATE(installment_valid_through), DATE(installment_valid_since), DAY) + 1
                                )
                            END
                        )
                    ) * (CASE WHEN collected_premium_by_installment.hp_class = d.hp_class THEN 1 ELSE 0 END) AS premium_collected_and_accrued_to_date
                FROM (
                    SELECT
                        amendment_id,
                        installment_number,
                        plan_groups.hp_class,
                        MIN(installment_valid_since) AS installment_valid_since,
                        MIN(installment_valid_through) AS installment_valid_through,
                        MIN(amendment_cancelation_date) AS amendment_cancelation_date,
                        MIN(cancelation_date) AS plan_cancelation_date,
                        SUM(installment_net_premium_before_discount) AS premium_collected_to_date
                    FROM dashboards_views.dashboards_ue_03_premium_collections AS p
                    LEFT JOIN plan_groups USING(healthplan_id)
                    LEFT JOIN hp_filters USING(healthplan_id)
                    WHERE p.transaction_date IS NOT NULL
                      AND p.transaction_date <= d.date_date
                      AND p.amendment_subscription_date <= d.date_date
                      AND p.subscription_date <= d.date_date
                      AND hp_filters.healthplan_id IS NOT NULL
                    GROUP BY 1, 2, 3
                ) AS collected_premium_by_installment
            ) AS collected_and_accrued
        ) AS accrued_and_collected_premium
    FROM date_hp_combinations AS d
),

date_ss_hp_combinations AS (
    SELECT *
    FROM date_cutoffs AS d
    CROSS JOIN (SELECT DISTINCT hp_class FROM plan_groups) AS aux1
    CROSS JOIN (SELECT DISTINCT ss_class FROM service_groups) AS aux2
),

service_payments_exploded AS (
    SELECT
        d.date_date,
        d.hp_class,
        d.ss_class,
        ROUND(COALESCE(SUM(ss.subtotal), 0.00), 2) AS service_payments
    FROM date_ss_hp_combinations AS d
    LEFT JOIN service_payments_in_scope AS ss ON ss.transaction_date <= d.date_date
        AND ss.hp_class = d.hp_class
        AND ss.ss_class = d.ss_class
    GROUP BY 1, 2, 3
),

high_granularity AS (
    SELECT
        ss.*,
        ROUND(p.accrued_and_collected_premium, 2) AS accrued_and_collected_premium
    FROM service_payments_exploded AS ss
    LEFT JOIN accrued_collected_premium_by_hp_class_and_date AS p USING(date_date, hp_class)
),

mid_granularity AS (
    SELECT
        date_date,
        CASE
            WHEN hp_class = 'B2C' THEN 'B2C'
            WHEN hp_class = 'B2B (1 - 9)' THEN 'B2B (1 - 9)'
            ELSE 'B2B (10 or more)'
        END AS hp_class,
        ss_class,
        ROUND(SUM(service_payments), 2) AS service_payments,
        ROUND(SUM(accrued_and_collected_premium), 2) AS accrued_and_collected_premium
    FROM high_granularity
    GROUP BY 1, 2, 3
),

low_granularity AS (
    SELECT
        date_date,
        CASE WHEN hp_class = 'B2C' THEN 'B2C' ELSE 'B2B' END AS hp_class,
        ss_class,
        ROUND(SUM(service_payments), 2) AS service_payments,
        ROUND(SUM(accrued_and_collected_premium), 2) AS accrued_and_collected_premium
    FROM high_granularity
    GROUP BY 1, 2, 3
),

no_granularity AS (
    SELECT
        date_date,
        'Overall' AS hp_class,
        ss_class,
        ROUND(SUM(service_payments), 2) AS service_payments,
        ROUND(SUM(accrued_and_collected_premium), 2) AS accrued_and_collected_premium
    FROM high_granularity
    GROUP BY 1, 2, 3
),

all_granularities AS (
    SELECT *
    FROM (
        SELECT 0 AS granularity_level, * FROM no_granularity
        UNION ALL
        SELECT 1 AS granularity_level, * FROM low_granularity
        UNION ALL
        SELECT 2 AS granularity_level, * FROM mid_granularity
        UNION ALL
        SELECT 3 AS granularity_level, * FROM high_granularity
    ) AS AUX
)

SELECT
    *,
    ROUND(service_payments / NULLIF(accrued_and_collected_premium, 0.00), 4) AS mlr_running_total,
    ROUND((service_payments - LAG(service_payments, 1) OVER w) / NULLIF(accrued_and_collected_premium - LAG(accrued_and_collected_premium, 1) OVER w, 0.00), 4) AS mlr_last_1_months,
    ROUND((service_payments - LAG(service_payments, 3) OVER w) / NULLIF(accrued_and_collected_premium - LAG(accrued_and_collected_premium, 3) OVER w, 0.00), 4) AS mlr_last_3_months,
    ROUND((service_payments - LAG(service_payments, 6) OVER w) / NULLIF(accrued_and_collected_premium - LAG(accrued_and_collected_premium, 6) OVER w, 0.00), 4) AS mlr_last_6_months,
    ROUND((service_payments - LAG(service_payments, 12) OVER w) / NULLIF(accrued_and_collected_premium - LAG(accrued_and_collected_premium, 12) OVER w, 0.00), 4) AS mlr_last_12_months
FROM all_granularities
WINDOW w AS (PARTITION BY granularity_level, hp_class, ss_class ORDER BY date_date ASC)
