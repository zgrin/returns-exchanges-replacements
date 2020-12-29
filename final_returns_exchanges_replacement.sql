-- Retrieve the most recent updates in loop_returns.data
DROP TABLE IF EXISTS most_recent_data;
CREATE TEMP TABLE most_recent_data AS
SELECT
    most_recent.*
FROM
    (SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_name, id
                        ORDER BY label_updated_at DESC) last_updated -- Locate most recently updated __sdc_primary_key per order_name
    FROM loop_returns.data
    ORDER BY label_updated_at DESC) most_recent
WHERE most_recent.last_updated = 1 -- Return only the most recent.
;

-- Join with line items details to get return/exchange item level data.
DROP TABLE IF EXISTS most_recent_line_items;
CREATE TEMP TABLE most_recent_line_items AS
SELECT
    a.*
FROM (
SELECT
    d.*,
    l.discount, l.exchange_variant, l.line_item_id, l.price, l.product_id, l.provider_line_item_id,
    l.return_reason, l.returned_at, l.tax, l.title, l.variant_id, l.parent_return_reason, l.barcode, l.outcome,
    CASE
        WHEN sku = 'RP8101' THEN 'RUG-PAD-NUL-RCT-RBR-RP001-08x10' -- Correct rug pad sku.
        ELSE sku
    END sku,
           ROW_NUMBER() OVER (PARTITION BY d.order_name, l.line_item_id
                        ORDER BY d.label_updated_at DESC) item_order_rank -- Assign row number per order, line_item_id to ensure no duplicates
FROM most_recent_data d
INNER JOIN loop_returns.data__line_items l
ON d.__sdc_primary_key = l._sdc_source_key___sdc_primary_key) a
WHERE item_order_rank = 1;

-- Left join distinct exchange data to identify exchanges.
DROP TABLE IF EXISTS all_returns_exchange;
CREATE TEMP TABLE all_returns_exchange AS
SELECT
    d.*,
    e.exchange_order_name,
    e2.total_items_exchanged_for,
    CASE
        WHEN e2.total_items_exchanged_for > 0 THEN 'exchange'
        ELSE 'return'
    END send_back_type, -- Exchanges defined as returns located in data__exchanges. Aggregation used to identify exchanges.
    ROW_NUMBER() OVER (PARTITION BY order_name, sku
                        ORDER BY created_at DESC) row_num_item_loop -- Assign row number to assist with cases where there a multiple returns/exchanges in same order.
FROM most_recent_line_items d
LEFT JOIN (SELECT
    DISTINCT _sdc_source_key___sdc_primary_key, exchange_order_name
    FROM loop_returns.data__exchanges) e
ON d.__sdc_primary_key = e._sdc_source_key___sdc_primary_key
LEFT JOIN (SELECT
    _sdc_source_key___sdc_primary_key, COUNT(sku) total_items_exchanged_for
    FROM loop_returns.data__exchanges GROUP BY 1) e2
ON d.__sdc_primary_key = e2._sdc_source_key___sdc_primary_key;


DROP TABLE IF EXISTS product_pull.ret_exch_rep_data_final;
CREATE TABLE product_pull.ret_exch_rep_data_final AS
SELECT
    a.email, a.cancelled_at, a.order_number, b.order_name, a.created_at_time, a.created_date, a.created_week_ended, a.created_month_ended, a.gross_sales, a.discount,
    a.disc_equals_gross, a.variant_title, a.product_type, a.product_sub_type, a.quantity, a.status, a.sku, a.design_name, a.size, a.plant,
    a.country, a.shape, a.texture, a.purpose, a.discount_code, a.order_id, a.line_id, a.line_order_id, a.component_sku_id, a.intended_line_item_id,
    a.row_num_component, a.row_num_item, a.row_num_variant, a.fulfillment_date, a.fulfillment_week_ended, a.fulfillment_month_ended,
    COALESCE(a.return_created_date::DATE, b.created_at::DATE) return_created_date,
--     a.return_received_date,
    CASE
        WHEN a.return_received_date::DATE IS NULL AND b.label_status = 'delivered' THEN TRIM(LEFT(label_updated_at, CHARINDEX('T', label_updated_at)::INT - 1))::DATE
        WHEN a.return_received_date::DATE IS NULL AND b.label_status = 'N/A' THEN b.returned_at::DATE -- May consider changing this to either N/A or some other value..T
        ELSE a.return_received_date::DATE
    END return_receive_date,
    a.secondary_return_reason_id, a.primary_return_reason_id, a.secondary_reason, a.primary_reason,
    b.return_reason loop_return_reason_detail, b.__sdc_primary_key,
    CASE
        WHEN COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND b.send_back_type IS NULL THEN 'return'
        ELSE b.send_back_type
    END send_back__type,
    CASE
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type = 'exchange')  AND (a.discount_code IS NOT NULL) AND (__sdc_primary_key IS NULL OR b.return_product_total::FLOAT = 0.00) THEN  a.gross_sales::FLOAT - a.discount::FLOAT
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type = 'exchange')  AND (a.discount_code IS NOT NULL) AND (__sdc_primary_key IS NOT NULL OR b.return_product_total::FLOAT != 0.00) THEN b.return_product_total::FLOAT - b.return_discount_total::FLOAT
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type = 'exchange')  AND (a.discount_code IS NULL) THEN a.gross_sales::FLOAT - a.discount::FLOAT
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type != 'exchange') AND (a.discount_code IS NOT NULL) AND (__sdc_primary_key IS NULL OR b.return_product_total::FLOAT = 0.00) THEN a.gross_sales::FLOAT - a.discount::FLOAT
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type != 'exchange') AND (a.discount_code IS NOT NULL) AND (__sdc_primary_key IS NOT NULL OR b.return_product_total::FLOAT != 0.00) THEN b.return_product_total::FLOAT - b.return_discount_total::FLOAT
        WHEN (COALESCE(a.return_created_date::DATE, b.created_at::DATE) IS NOT NULL AND return_receive_date::DATE IS NOT NULL AND send_back__type != 'exchange') AND (a.discount_code IS NULL) THEN a.gross_sales::FLOAT - a.discount::FLOAT
        ELSE 0
    END refund,
    b.exchange_credit_total, b.exchange_discount_total, b.exchange_product_total, b.exchange_tax_total,
    b.exchange_total, b.return_credit_total, b.return_discount_total, b.return_product_total, b.return_tax_total,
    b.return_total, b.gift_card, b.tax, b.handling_fee, b.upsell, b.price, b.total, b.refund loop_refund, b.label_updated_at,
    b.label_status, b.state,  b.tracking_number, b.returned_at,  b.total_items_exchanged_for, b.row_num_item_loop
FROM product_pull.return_exchange_replacement_data a
LEFT JOIN all_returns_exchange b
    ON a.order_number = TRIM(REPLACE(b.order_name, '#', ''))
        AND a.sku = b.sku
        AND a.row_num_item = b.row_num_item_loop;

GRANT SELECT ON product_pull.ret_exch_rep_data_final TO james_graham;

SELECT DISTINCT primary_reason FROM product_pull.ret_exch_rep_data_final

SELECT DISTINCT primary_reason, secondary_reason, loop_return_reason_detail FROM product_pull.ret_exch_rep_data_final

SELECT DISTINCT loop_return_reason_detail FROM product_pull.ret_exch_rep_data_final WHERE secondary_reason ILIKE '%unauth%'