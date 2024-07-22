--CREATE OR REPLACE TABLE dashboards_views.dashboards_ue_03_premium_collections AS
-- ACCURACY NO COMPROBADO. NO SE ASEGURA IGUALDAD ENTRE LA TABLA GENERADA EN POSTGRES Y BIGQUERY

WITH content_type_aux AS (
    SELECT
        id
    FROM
        `backend_db_20240702.django_content_type`
    WHERE
        model = 'healthplanamendment'
),

invoiceitems_in_scope AS (
    SELECT
        ii.id,
        object_id,
        ROUND(subtotal_cents / 100.00, 2) AS installment_subtotal,
        ROUND(net_premium_cents / 100.00, 2) AS installment_net_premium,
        ROUND(total_cents / 100.00, 2) AS installment_total,
        installment_number,
        payment_flow,
        valid_since,
        valid_through
    FROM
        `backend_db_20240702.invoice_invoiceitem` AS ii
        LEFT JOIN content_type_aux AS ct ON ii.content_type_id = ct.id
    WHERE
        ii.deleted IS NULL
        AND ct.id IS NOT NULL
),

transactions_in_scope AS (
    SELECT
        id,
        invoice_id,
        transaction_type,
        confirmed_at
    FROM
        `backend_db_20240702.payments_transaction`
    WHERE
        transaction_type IN ('DB', 'RF', 'FW', 'PY', 'PR')
        AND payment_gateway_result = 'OK'
        AND deleted IS NULL
        AND invoice_id IS NOT NULL
)

SELECT
    healthplan_id,
    member_id,
    age_at_subscription,
    gender,
    product_family,
    product_id,
    is_collective,
    insured_amount,
    subscription_date,
    cancelation_date,
    is_internal,
    healthplan_state,
    subscription_type,
    vertical,
    business_id,
    business_group,
    business_group_size,
    business_name,
    amendment_id,
    amendment_category,
    amendment_valid_since,
    amendment_valid_through,
    amendment_subscription_date,
    amendment_cancelation_date,
    op.net_premium_before_discount AS amendment_net_premium_before_discount,
    net_premium AS amendment_net_premium,
    op.subtotal AS amendment_subtotal,
    total AS amendment_total,
    installment_number,
    op.net_premium_before_discount * (installment_net_premium / op.net_premium) * (CASE WHEN transaction_type IN ('PR','RF','FW') THEN -1 ELSE 1 END) AS installment_net_premium_before_discount,
    installment_net_premium * (CASE WHEN transaction_type IN ('PR','RF','FW') THEN -1 ELSE 1 END) AS installment_net_premium,
    installment_subtotal * (CASE WHEN transaction_type IN ('PR','RF','FW') THEN -1 ELSE 1 END) AS installment_subtotal,
    installment_total * (CASE WHEN transaction_type IN ('PR','RF','FW') THEN -1 ELSE 1 END) AS installment_total,
    ii.valid_since AS installment_valid_since,
    ii.valid_through AS installment_valid_through,
    pt.id AS transaction_id,
    pt.transaction_type,
    DATE(pt.confirmed_at, 'America/Mexico_City') AS transaction_date -- Applying timezone conversion directly
FROM
    dashboards_views.dashboards_ue_01_premium AS op
    LEFT JOIN invoiceitems_in_scope AS ii ON ii.object_id = op.amendment_id
    LEFT JOIN `backend_db_20240702.invoice_invoiceitem_invoice` AS iii ON iii.invoiceitem_id = ii.id
    LEFT JOIN transactions_in_scope AS pt ON pt.invoice_id = iii.invoice_id
