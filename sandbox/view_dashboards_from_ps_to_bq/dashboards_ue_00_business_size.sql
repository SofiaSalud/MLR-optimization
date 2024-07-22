--CREATE OR REPLACE TABLE dashboards_views.dashboards_ue_00_business_size AS
-- ACCURACY NO COMPROBADO. NO SE ASEGURA IGUALDAD ENTRE LA TABLA GENERADA EN POSTGRES Y BIGQUERY

WITH 
cancelation_dates AS (
  SELECT
      h.id AS healthplan_id,
      MIN
      (
          CASE WHEN COALESCE(ca.signed_at, h.cancelled_at) IS NOT NULL THEN
              DATE(LEAST(h.valid_through, COALESCE(ca.signed_at, h.cancelled_at)), 'America/Mexico_City')
          END
      ) AS cancelled_at
  FROM
      backend_db_20240702.healthplan_healthplan as h
      LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS a ON h.id = a.health_plan_id
      LEFT JOIN backend_db_20240702.healthplan_healthplanamendment AS ca ON ca.cancelled_amendment_id = a.id 
  WHERE
      h.is_owned_by_business = TRUE
      AND a.review_result = 'AP'
      AND a.amendment_category IN ('SA', 'CS', 'AS') 
      AND a.deleted IS NULL 
      AND h.deleted IS NULL
      AND (ca.id IS NULL OR (ca.review_result = 'AP' AND ca.deleted IS NULL AND ca.amendment_category IN ('CN','DB','FR')))
  GROUP BY healthplan_id
),
base AS (
    SELECT DISTINCT
        h.id as healthplan_id,
        ba.business_id,
        TRIM(COALESCE(b.common_name, b.legal_name)) AS business_name,
        h.beneficiary_member_id AS member_id,
        DATE(h.signed_at, 'America/Mexico_City') AS subscription_date,
        COALESCE(
            c.cancelled_at,
            CASE 
                WHEN DATE(h.valid_through, 'America/Mexico_City') < CURRENT_DATE('America/Mexico_City') THEN DATE(h.valid_through, 'America/Mexico_City') 
            END 
        ) AS cancelation_date,
        CASE 
            WHEN ih.internal IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_internal,	
        h.state AS healthplan_state,
        h.is_owned_by_business
    FROM
        backend_db_20240702.healthplan_healthplan AS h
        LEFT JOIN common_20240702.internal_hps AS ih 
            ON ih.internal = h.id 
        LEFT JOIN backend_db_20240702.healthplan_healthplan AS h2 
            ON h2.previous_health_plan_id = h.id
        LEFT JOIN cancelation_dates as c 
            ON h.id = c.healthplan_id
        LEFT JOIN backend_db_20240702.business_businessadmin AS ba 
            ON ba.role = 'OW' AND ba.user_id = h.owner_user_id AND h.is_owned_by_business = TRUE 
        LEFT JOIN backend_db_20240702.business_business AS b 
            ON ba.business_id = b.id 
    WHERE
        h.deleted IS NULL 
        AND h2.deleted IS NULL 
        AND h.is_owned_by_business
),
active_or_not AS (
    SELECT
        business_id,
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN healthplan_state = 'AC' THEN member_id END) > 0 THEN 'AC'
            ELSE 'IN'
        END AS business_state
    FROM
        base
    GROUP BY
        1
),
base_with_business_canceldates AS (
SELECT
	base.*,
	active_or_not.business_state,
	CASE WHEN active_or_not.business_state = 'IN' THEN 
		MAX(cancelation_date) OVER (PARTITION BY business_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
	END AS business_cancelation_date
FROM
	base
	LEFT JOIN active_or_not USING (business_id) 
),
adding_groups AS (
	SELECT
		COALESCE(g.group, business_name) AS business_group,
		b.*
	FROM
		base_with_business_canceldates AS b
    LEFT JOIN common_20240702.business_groups AS g 
    USING (business_id)
),
groups_and_ids AS (
	SELECT
		business_group,
		ARRAY_AGG(DISTINCT business_id) AS business_ids
	FROM
		adding_groups
	GROUP BY 1
),
adding_groups_enriched AS (
	SELECT
		*,
		max(business_cancelation_date) OVER (PARTITION BY business_group RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as business_group_cancelation_date,
		min(subscription_date) OVER (PARTITION BY business_group RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as business_group_incorporation_date
	FROM
		adding_groups
),
allcombs AS (
	SELECT  
        *
    FROM(
        SELECT DISTINCT 
            * 
        FROM(
        (SELECT DISTINCT business_group, subscription_date AS date FROM adding_groups_enriched)
        UNION ALL 
        (SELECT DISTINCT business_group, cancelation_date AS date FROM adding_groups_enriched)
        UNION ALL 
        (SELECT DISTINCT business_group, CURRENT_DATE('America/Mexico_City') AS date FROM adding_groups_enriched)
        ) AS U
	) AS d 
	WHERE date IS NOT NULL 
),
max_simultaneous AS (
    SELECT
        business_group,
        MAX(total_active_socios_hp_by_eod) as max_simultaneous_members
    FROM 
        (
        SELECT 
            business_group,
            date,
            COUNT(DISTINCT member_id) AS total_active_socios_hp_by_eod
        FROM 
            (
            SELECT
                c.*,
                b.member_id,
                b.subscription_date,
                b.cancelation_date
            FROM
                allcombs AS c
                LEFT JOIN adding_groups_enriched AS b ON
                    b.business_group = c.business_group
                    AND b.subscription_date <= c.date
                    AND (b.cancelation_date IS NULL OR b.cancelation_date > c.date)
            ) AS AUX
        GROUP BY 1, 2
        ) AS AUX 
    GROUP BY 1
),
m_today AS (
	SELECT
		business_group,
		count(DISTINCT member_id) as members_today
	FROM
		adding_groups_enriched
	WHERE
		healthplan_state = 'AC'
	GROUP BY 1
),
base_after_30_days AS (
    SELECT DISTINCT
        business_group, business_group_incorporation_date, business_group_cancelation_date
    FROM
        adding_groups_enriched
),
after_30_days AS (
	SELECT
		b.business_group,
		(
        SELECT 
            count(DISTINCT member_id)
        FROM 
            adding_groups_enriched as b2 
        WHERE
            b2.business_group=b.business_group 
            AND b2.subscription_date <= b.business_group_incorporation_date + 30 
            AND (b2.cancelation_date IS NULL OR b2.cancelation_date  > b.business_group_incorporation_date + 30)
		
		) as active_after_30_days
	FROM
		base_after_30_days as b
	
),
all_together AS (
    SELECT
        business_group,
        max_simultaneous_members,
        COALESCE(active_after_30_days,0) AS members_30_days_after_incorporation,
        COALESCE(members_today,0) AS members_as_of_today,
        CASE 
            WHEN COALESCE(members_today,0) = 0 THEN 'Inactive' 
            ELSE 'Active' 
        END AS business_group_state
    FROM
        max_simultaneous
    FULL JOIN m_today 
        USING (business_group)
    FULL JOIN after_30_days 
        USING (business_group)
),
main AS (
    SELECT
        business_group,
        gid.business_ids,
        business_group_incorporation_date,
        CASE 
            WHEN members_as_of_today = 0 THEN business_group_cancelation_date 
        END AS business_group_cancelation_date,
        members_30_days_after_incorporation,
        max_simultaneous_members,
        members_as_of_today,
        CASE 
            WHEN members_30_days_after_incorporation < 10 THEN '1 - 9'
            WHEN members_30_days_after_incorporation < 20 THEN '10 - 19'	
            WHEN members_30_days_after_incorporation < 50 THEN '20 - 49'	
            ELSE '50 or more'
        END AS members_30_days_after_incorporation_range,
        CASE 
            WHEN max_simultaneous_members < 10 THEN '1 - 9'
            WHEN max_simultaneous_members < 20 THEN '10 - 19'	
            WHEN max_simultaneous_members < 50 THEN '20 - 49'	
            ELSE '50 or more'
        END AS max_simultaneous_members_range,
        CASE 
            WHEN members_as_of_today < 10 THEN '1 - 9'
            WHEN members_as_of_today < 20 THEN '10 - 19'	
            WHEN members_as_of_today < 50 THEN '20 - 49'	
            ELSE '50 or more'
        END AS members_as_of_today_range
    FROM
        all_together
    LEFT JOIN base_after_30_days 
        USING (business_group)
    LEFT JOIN groups_and_ids AS gid 
        USING(business_group)
)

SELECT
	*
FROM
	main
