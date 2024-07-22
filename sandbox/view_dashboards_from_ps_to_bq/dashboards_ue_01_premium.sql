--CREATE OR REPLACE TABLE dashboards_views.dashboards_ue_01_premium AS
-- ACCURACY NO COMPROBADO. NO SE ASEGURA IGUALDAD ENTRE LA TABLA GENERADA EN POSTGRES Y BIGQUERY

WITH 
cancelation_dates AS (
	SELECT
		h.id AS healthplan_id,
		MIN
		(
			CASE 
				WHEN COALESCE(ca.signed_at, h.cancelled_at) IS NOT NULL THEN LEAST(h.valid_through, COALESCE(ca.signed_at, h.cancelled_at))
			END
		) AS cancelled_at
	FROM
		backend_db_20240702.healthplan_healthplan as h
		LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS a 
			ON h.id = a.health_plan_id
		LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS ca 
			ON ca.cancelled_amendment_id = a.id 
	WHERE
		a.review_result = 'AP'
		AND a.amendment_category IN ('SA', 'CS', 'AS') 
		AND a.deleted IS NULL 
		AND h.deleted IS NULL
		AND (ca.id IS NULL OR (ca.review_result = 'AP' AND ca.deleted IS NULL AND ca.amendment_category IN ('CN','DB','FR')))
	GROUP BY 1
),
cancelation_reasons AS (
	SELECT
		h.id as healthplan_id,
		COALESCE(c.description, 'Other / not specified') AS cancel_reason
	FROM
		 backend_db_20240702.healthplan_healthplanamendment AS sa
		 LEFT JOIN backend_db_20240702.healthplan_healthplan AS h 
		 	ON h.id = sa.health_plan_id
		 LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS ca 
		 	ON ca.cancelled_amendment_id = sa.id 
		 LEFT JOIN common_20240702.cancel_reasons AS c 
		 	ON JSON_EXTRACT(ca.amendment_extras, '$.cancel_reason') = c.code
	WHERE
		sa.amendment_category in ('SA', 'CS', 'AS')
		AND sa.review_result = 'AP'
		AND sa.signed_at IS NOT NULL
		AND sa.deleted IS NULL
		AND ca.review_result = 'AP'
		AND ca.signed_at IS NOT NULL
		AND ca.amendment_category IN ('CN','DB','FR')
		AND ca.deleted IS NULL 
),
cancelations AS (
	SELECT
		cancelation_dates.*,
		COALESCE(cancelation_reasons.cancel_reason, 'Other / not specified') AS cancel_reason
	FROM
		cancelation_dates
		LEFT JOIN cancelation_reasons 
			USING(healthplan_id)
	WHERE
		cancelation_dates.cancelled_at IS NOT NULL
),
base AS (
	SELECT DISTINCT
		h.id as healthplan_id,
		CASE 
			WHEN h.is_owned_by_business = TRUE THEN 'B2B' 
			ELSE 'B2C' 
		END AS vertical,
		p.product_family,
		ba.business_id,
		TRIM(COALESCE(b.common_name, b.legal_name)) AS business_name,
		bz.business_group,
		bz.max_simultaneous_members_range AS business_group_size,
		CASE 
			WHEN h.collective_health_plan_id IS NULL THEN FALSE 
			ELSE TRUE 
		END as is_collective,
		h.beneficiary_member_id AS member_id,
		CASE 
			WHEN p.product_family = 'mini' THEN '0.3M' -- patche
			ELSE CONCAT(ROUND(CAST(JSON_EXTRACT(h.product_parameters_values, '$.maximumCoverageCents') AS FLOAT64) /100000000.00,1), 'M')
		END AS insured_amount,
		DATE(h.signed_at, 'America/Mexico_City') AS subscription_date,
		CASE 
			WHEN CURRENT_DATE('America/Mexico_City') > DATE(h.valid_through, 'America/Mexico_City') OR c.cancelled_at IS NOT NULL THEN DATE(COALESCE(c.cancelled_at, h.valid_through), 'America/Mexico_City')
		END AS cancelation_date, 	
		COALESCE(cancel_reason, 
			CASE 
				WHEN CURRENT_DATE('America/Mexico_City') > DATE(h.valid_through, 'America/Mexico_City') AND h2.id IS NULL AND h.state = 'IN' THEN 'Did not renew' 
				WHEN (h.inactive_reason = 'RN' OR h2.id is not null) THEN 'Plan was renewed' 
			END
		) AS cancel_reason,
		DATE_DIFF(
			LEAST(CURRENT_DATE('America/Mexico_City'), DATE(h.valid_through, 'America/Mexico_City'), DATE(c.cancelled_at, 'America/Mexico_City')), 
			DATE(h.valid_since, 'America/Mexico_City'), 
			DAY
		) + 1 as healthplan_vigour_days_so_far,
		(DATE_DIFF(
			LEAST(CURRENT_DATE('America/Mexico_City'), DATE(h.valid_through, 'America/Mexico_City'), DATE(c.cancelled_at, 'America/Mexico_City')), 
			DATE(h.valid_since, 'America/Mexico_City'),
			DAY
		) + 1)
		/
		(DATE_DIFF(
			DATE(h.valid_through, 'America/Mexico_City'), 
			DATE(h.valid_since, 'America/Mexico_City'),
			DAY
		) + 1) AS healthplan_vigour_days_so_far_prop,
		CASE 
			WHEN ih.internal IS NOT NULL OR h.owner_user_id IN (1505, 14189, 29326) THEN TRUE 
			ELSE FALSE 
		END AS is_internal,
		CASE 
			WHEN h.state ='AC' AND CURRENT_DATE('America/Mexico_City') < DATE(h.valid_through, 'America/Mexico_City') THEN 'Active' 
			ELSE 'Inactive' 
		END as healthplan_state,
		h.subscription_type,
		DATE_DIFF(DATE(h.signed_at, 'America/Mexico_City'), PARSE_DATE('%F', m.date_of_birth), YEAR) AS age_at_subscription,
		m.gender
	FROM
		backend_db_20240702.healthplan_healthplan AS h
		LEFT JOIN common_20240702.internal_hps AS ih 
			ON ih.internal = h.id
		LEFT JOIN backend_db_20240702.healthplan_healthplan AS h2 
			ON h2.previous_health_plan_id = h.id
		LEFT JOIN cancelations AS c 
			ON h.id = c.healthplan_id
		LEFT JOIN backend_db_20240702.sofia_app_member AS m 
			ON h.beneficiary_member_id = m.id
		LEFT JOIN backend_db_20240702.subscriptions_product AS p 
			ON h.product_id = p.id 
		LEFT JOIN backend_db_20240702.business_businessadmin AS ba 
			ON ba.role = 'OW' AND ba.user_id = h.owner_user_id AND h.is_owned_by_business = TRUE 
		LEFT JOIN backend_db_20240702.business_business AS b 
			ON ba.business_id = b.id 
		LEFT JOIN dashboards_views.dashboards_ue_00_business_size as bz 
			ON b.id IN UNNEST(bz.business_ids)
	WHERE
		h.deleted IS NULL 
		AND h2.deleted IS NULL 
),
amendments AS (
    SELECT 
        a.id AS amendment_id,
        a.health_plan_id AS healthplan_id,
        CASE 
					WHEN c.legend IN ('Collective Subscription Amendment', 'Subscription Amendment') THEN 'Base Coverage'
        	ELSE initcap(c.legend)
        END AS amendment_category,
        DATE(a.signed_at, 'America/Mexico_City') AS amendment_subscription_date,
        DATE(ca.signed_at, 'America/Mexico_City') AS amendment_cancelation_date,
        DATE(a.valid_since, 'America/Mexico_City') AS amendment_valid_since,
        DATE(a.valid_through, 'America/Mexico_City') AS amendment_valid_through,
        ROUND(
						CASE 
							WHEN a.amendment_category = 'MDT' THEN a.premium_subtotal_difference_cents 
							ELSE a.premium_net_premium_difference_cents 
						END
				/100.00, 2) AS net_premium,
        ROUND(a.premium_subscription_type_surcharge_difference_cents/100.00, 2) AS surcharge,
        ROUND(a.premium_subtotal_difference_cents/100.00, 2) AS subtotal,
        ROUND(a.premium_total_difference_cents/100.00, 2) AS total
    FROM
        backend_db_20240702.healthplan_healthplanamendment AS a 
        LEFT JOIN common_20240702.amendment_type_codes AS c 
					ON a.amendment_category = c.code
        LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS ca 
					ON ca.cancelled_amendment_id = a.id 
            AND ca.amendment_category IN ('CN','DB','FR','AC')
            AND ca.review_result = 'AP'
            AND ca.deleted IS NULL 
    WHERE
        a.premium_total_difference_cents > 0 -- Solo los que suscriben prima
        AND a.deleted IS NULL 
        AND a.review_result = 'AP'
),
discounts AS (
	SELECT
		h.id AS healthplan_id,
		COALESCE(TRUNC(CAST(JSON_EXTRACT(h.product_parameters_values, '$.discountPercent') AS FLOAT64) / 100.00, 4), 0.0000) AS discount
	FROM
		backend_db_20240702.healthplan_healthplan AS h
	WHERE
		h.deleted IS NULL
),
main AS (
-- Ojo, es a nivel amendment... asi que puede haber varios rows por 1 hp
SELECT
	b.*,
	h.product_id,
	a.amendment_id,
	a.amendment_valid_since,
	a.amendment_valid_through,
	a.amendment_subscription_date,
	COALESCE(amendment_cancelation_date , b.cancelation_date) AS amendment_cancelation_date,
	a.amendment_category,

	DATE_DIFF(
		LEAST(CURRENT_DATE('America/Mexico_City'), amendment_valid_through, COALESCE(amendment_cancelation_date , b.cancelation_date)),
		a.amendment_valid_since,
		DAY
	) + 1 AS amendment_vigour_days_so_far,
	(DATE_DIFF(
		LEAST(CURRENT_DATE('America/Mexico_City'), amendment_valid_through, COALESCE(amendment_cancelation_date , b.cancelation_date)),
		a.amendment_valid_since,
		DAY
	) + 1)
	/
	(DATE_DIFF(
		amendment_valid_through, 
		amendment_valid_since,
		DAY
	) + 1) AS amendment_vigour_days_so_far_prop,
	CASE 
		WHEN a.amendment_category IN ('Base Coverage', 'Maternity Add-On') THEN d.discount 
		ELSE 0.0000
	END AS discountpercent,
	CASE 
		WHEN nch.healthplan_uuid IS NOT NULL OR p.product_family = 'mini' OR a.amendment_category NOT IN ('Base Coverage', 'Maternity Add-On') THEN FALSE 
		ELSE TRUE 
	END AS reinsurance_participates,
	CASE 
		WHEN a.amendment_category IN ('Base Coverage', 'Maternity Add-On') AND d.discount > 0 THEN ROUND(a.net_premium / (1.0 - d.discount), 2)
		ELSE a.net_premium 
	END AS net_premium_before_discount,
	CASE 
		WHEN a.amendment_category IN ('Base Coverage', 'Maternity Add-On') AND d.discount > 0 THEN ROUND(ROUND(a.net_premium / (1.0 - d.discount), 2) - a.net_premium, 2)
		ELSE 0.00
	END AS discount,
	a.net_premium,
	a.surcharge,
	a.subtotal,
	a.total
FROM
	base AS b
	LEFT JOIN discounts AS d ON b.healthplan_id = d.healthplan_id
	LEFT JOIN amendments AS a ON b.healthplan_id = a.healthplan_id
	LEFT JOIN backend_db_20240702.healthplan_healthplan AS h ON a.healthplan_id = h.id 
	LEFT JOIN backend_db_20240702.subscriptions_product AS p on h.product_id = p.id 
	LEFT JOIN common_20240702.non_ceded_healthplans as nch ON nch.healthplan_uuid = h.uuid
)

SELECT 
	* 
FROM 
	main 
