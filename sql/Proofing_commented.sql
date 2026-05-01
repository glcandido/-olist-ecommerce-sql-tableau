/*
  Olist E-commerce Analytics (MySQL)
  ============================================================
  Step 2: Profiling + Clean Layer Build
  Step 3: Core Facts/Marts + Revenue Validation (GMV proxy)

  Design principles
  - RAW tables remain immutable.
  - *_clean tables are recreated as a consistent, analysis-ready “silver” layer.
  - Analytical tables (facts/marts) are built on top of *_clean tables.

  Execution notes
  - This script is written to be re-runnable. It DROP/CREATEs build artifacts.
  - Run top-to-bottom. Optional diagnostics are included as commented blocks.
*/

/* ======================================================================
   STEP 2A — RAW PROFILING (SANITY GATES)
   Purpose: Validate import integrity before building clean tables.
   ====================================================================== */

/* 2A.1 Row counts across key RAW tables */
SELECT 'orders'    AS tbl, COUNT(*) AS row_count FROM olist_orders_dataset
UNION ALL SELECT 'items',     COUNT(*)            FROM olist_order_items_dataset
UNION ALL SELECT 'payments',  COUNT(*)            FROM olist_order_payments_dataset
UNION ALL SELECT 'customers', COUNT(*)            FROM olist_customers_dataset
UNION ALL SELECT 'products',  COUNT(*)            FROM olist_products_dataset
UNION ALL SELECT 'reviews',   COUNT(*)            FROM olist_order_reviews_dataset;

/* 2A.2 Key uniqueness checks (expected: orders.order_id unique) */
SELECT
  COUNT(*)                  AS orders,
  COUNT(DISTINCT order_id)   AS distinct_order_id
FROM olist_orders_dataset;

/* Reviews: review_id may not be unique depending on import/quality */
SELECT
  COUNT(*)                AS reviews,
  COUNT(DISTINCT review_id) AS distinct_review_id
FROM olist_order_reviews_dataset;

/* 2A.3 Missingness on critical order fields */
SELECT
  SUM(CASE WHEN order_purchase_timestamp IS NULL OR order_purchase_timestamp = '' THEN 1 ELSE 0 END) AS missing_purchase_ts,
  SUM(CASE WHEN order_delivered_customer_date IS NULL OR order_delivered_customer_date = '' THEN 1 ELSE 0 END) AS missing_delivered_ts,
  SUM(CASE WHEN order_status IS NULL OR order_status = '' THEN 1 ELSE 0 END) AS missing_status
FROM olist_orders_dataset;

/* 2A.4 Order status distribution (used later for delivered-only KPIs) */
SELECT order_status, COUNT(*) AS orders
FROM olist_orders_dataset
GROUP BY order_status
ORDER BY orders DESC;


/* ======================================================================
   STEP 2B — CLEAN TABLES (“SILVER” LAYER)
   Purpose: Create typed, join-friendly, analysis-ready tables.
   ====================================================================== */

/* 2B.1 Orders clean
   - Parse timestamps
   - Add delivery_days + is_delayed
*/
DROP TABLE IF EXISTS olist_orders_dataset_clean;
CREATE TABLE olist_orders_dataset_clean AS
SELECT
  order_id,
  customer_id,
  order_status,
  STR_TO_DATE(NULLIF(order_purchase_timestamp,''), '%Y-%m-%d %H:%i:%s')         AS purchase_ts,
  STR_TO_DATE(NULLIF(order_approved_at,''), '%Y-%m-%d %H:%i:%s')               AS approved_ts,
  STR_TO_DATE(NULLIF(order_delivered_carrier_date,''), '%Y-%m-%d %H:%i:%s')    AS carrier_ts,
  STR_TO_DATE(NULLIF(order_delivered_customer_date,''), '%Y-%m-%d %H:%i:%s')   AS delivered_ts,
  STR_TO_DATE(NULLIF(order_estimated_delivery_date,''), '%Y-%m-%d %H:%i:%s')   AS estimated_delivery_ts
FROM olist_orders_dataset;

ALTER TABLE olist_orders_dataset_clean
  /* Fix CSV-import TEXT IDs so they can be indexed */
  MODIFY order_id    CHAR(32) NOT NULL,
  MODIFY customer_id CHAR(32) NOT NULL,
  /* Derived fields for delivery performance */
  ADD COLUMN delivery_days DECIMAL(10,2),
  ADD COLUMN is_delayed    TINYINT,
  /* Keys/indexes for joins */
  ADD PRIMARY KEY (order_id),
  ADD INDEX idx_orders_customer_id (customer_id),
  ADD INDEX idx_orders_purchase_ts (purchase_ts),
  ADD INDEX idx_orders_status (order_status);

UPDATE olist_orders_dataset_clean
SET
  delivery_days = CASE
    WHEN purchase_ts IS NULL OR delivered_ts IS NULL THEN NULL
    ELSE TIMESTAMPDIFF(HOUR, purchase_ts, delivered_ts) / 24.0
  END,
  is_delayed = CASE
    WHEN delivered_ts IS NULL OR estimated_delivery_ts IS NULL THEN NULL
    WHEN delivered_ts > estimated_delivery_ts THEN 1
    ELSE 0
  END;

/* 2B.2 Customers clean
   - Normalise IDs
   - Keep customer_unique_id for repeat/retention analysis
*/
DROP TABLE IF EXISTS olist_customers_dataset_clean;
CREATE TABLE olist_customers_dataset_clean AS
SELECT
  TRIM(customer_id)        AS customer_id,
  TRIM(customer_unique_id) AS customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM olist_customers_dataset;

ALTER TABLE olist_customers_dataset_clean
  MODIFY customer_id        CHAR(32) NOT NULL,
  MODIFY customer_unique_id CHAR(32) NOT NULL,
  ADD PRIMARY KEY (customer_id),
  ADD INDEX idx_customers_unique_id (customer_unique_id);

/* 2B.3 Order items clean
   - Normalise IDs
   - Cast price/freight to DECIMAL
*/
DROP TABLE IF EXISTS olist_order_items_dataset_clean;
CREATE TABLE olist_order_items_dataset_clean AS
SELECT
  TRIM(order_id)  AS order_id,
  order_item_id,
  TRIM(product_id) AS product_id,
  TRIM(seller_id) AS seller_id,
  STR_TO_DATE(NULLIF(shipping_limit_date,''), '%Y-%m-%d %H:%i:%s') AS shipping_limit_ts,
  CAST(price AS DECIMAL(10,2)) AS price,
  CAST(freight_value AS DECIMAL(10,2)) AS freight_value
FROM olist_order_items_dataset;

ALTER TABLE olist_order_items_dataset_clean
  MODIFY order_id   CHAR(32) NOT NULL,
  MODIFY product_id CHAR(32) NOT NULL,
  MODIFY seller_id  CHAR(32) NOT NULL,
  ADD INDEX idx_items_order_id (order_id),
  ADD INDEX idx_items_product_id (product_id),
  ADD INDEX idx_items_seller_id (seller_id);

/* 2B.4 Payments clean
   - Normalise order_id
   - Cast payment_value to DECIMAL
*/
DROP TABLE IF EXISTS olist_order_payments_dataset_clean;
CREATE TABLE olist_order_payments_dataset_clean AS
SELECT
  TRIM(order_id) AS order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  CAST(payment_value AS DECIMAL(10,2)) AS payment_value
FROM olist_order_payments_dataset;

ALTER TABLE olist_order_payments_dataset_clean
  MODIFY order_id CHAR(32) NOT NULL,
  ADD INDEX idx_payments_order_id (order_id),
  ADD INDEX idx_payments_type (payment_type);

/* 2B.5 Reviews clean
   - Safe timestamp parsing: invalid values become NULL
   - Filter to valid IDs (handles corrupt imports)
   - Enforce composite uniqueness (review_id, order_id)
*/
/* Optional diagnostic: show un-parseable creation dates */
-- SELECT review_creation_date
-- FROM olist_order_reviews_dataset
-- WHERE review_creation_date IS NOT NULL
--   AND review_creation_date <> ''
--   AND STR_TO_DATE(review_creation_date, '%Y-%m-%d %H:%i:%s') IS NULL
-- LIMIT 50;

DROP TABLE IF EXISTS olist_order_reviews_dataset_clean;
CREATE TABLE olist_order_reviews_dataset_clean AS
SELECT
  TRIM(review_id) AS review_id,
  TRIM(order_id)  AS order_id,
  CAST(review_score AS UNSIGNED) AS review_score,
  CASE
    WHEN review_creation_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      THEN STR_TO_DATE(review_creation_date, '%Y-%m-%d %H:%i:%s')
    ELSE NULL
  END AS review_creation_ts,
  CASE
    WHEN review_answer_timestamp REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      THEN STR_TO_DATE(review_answer_timestamp, '%Y-%m-%d %H:%i:%s')
    ELSE NULL
  END AS review_answer_ts
FROM olist_order_reviews_dataset
WHERE TRIM(review_id) REGEXP '^[A-Za-z0-9]{32,64}$'
  AND TRIM(order_id)  REGEXP '^[A-Za-z0-9]{32}$';

ALTER TABLE olist_order_reviews_dataset_clean
  MODIFY review_id VARCHAR(64) NOT NULL,
  MODIFY order_id  CHAR(32) NOT NULL,
  ADD PRIMARY KEY (review_id, order_id),
  ADD INDEX idx_reviews_order_id (order_id),
  ADD INDEX idx_reviews_score (review_score);

/* 2B.6 Products + category translation
   - Fix BOM header in translation table
   - Create translation_clean
   - Create products_clean with category_english
*/

/* Translation header fix: some CSV imports create a hidden BOM character */
ALTER TABLE product_category_name_translation
  CHANGE COLUMN `﻿product_category_name` product_category_name TEXT;

DROP TABLE IF EXISTS product_category_name_translation_clean;
CREATE TABLE product_category_name_translation_clean AS
SELECT
  TRIM(product_category_name) AS product_category_name,
  TRIM(product_category_name_english) AS product_category_name_english
FROM product_category_name_translation;

CREATE INDEX idx_cat_pt ON product_category_name_translation_clean(product_category_name(32));

DROP TABLE IF EXISTS olist_products_dataset_clean;
CREATE TABLE olist_products_dataset_clean AS
SELECT
  TRIM(p.product_id) AS product_id,
  TRIM(p.product_category_name) AS product_category_name,
  t.product_category_name_english AS category_english,
  p.product_name_lenght,
  p.product_description_lenght,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM olist_products_dataset p
LEFT JOIN product_category_name_translation_clean t
  ON TRIM(p.product_category_name) = t.product_category_name;

ALTER TABLE olist_products_dataset_clean
  MODIFY product_id CHAR(32) NOT NULL,
  ADD PRIMARY KEY (product_id),
  ADD INDEX idx_products_category (category_english(32));

/* Optional: confirm build artifacts */
-- SHOW TABLES LIKE '%_clean';


/* ======================================================================
   STEP 3A — ANALYTICAL FOUNDATION (FACTS)
   Purpose: Create one-row-per-order datasets for BI and KPI modelling.
   ====================================================================== */

/* 3A.1 Orders enriched (orders + customers)
   Grain: one row per order
*/
DROP TABLE IF EXISTS orders_enriched;
CREATE TABLE orders_enriched AS
SELECT
  o.*,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state
FROM olist_orders_dataset_clean o
JOIN olist_customers_dataset_clean c
  ON o.customer_id = c.customer_id;

CREATE INDEX idx_orders_enriched_order_id ON orders_enriched(order_id);
CREATE INDEX idx_orders_enriched_unique_customer ON orders_enriched(customer_unique_id);
CREATE INDEX idx_orders_enriched_purchase_ts ON orders_enriched(purchase_ts);
CREATE INDEX idx_orders_enriched_status ON orders_enriched(order_status);

/* 3A.2 Order revenue (from order items)
   Grain: one row per order
   - revenue_price: price-only
   - revenue_price_freight: price + freight (GMV proxy)
*/
DROP TABLE IF EXISTS order_revenue;
CREATE TABLE order_revenue AS
SELECT
  oi.order_id,
  SUM(oi.price) AS revenue_price,
  SUM(oi.price + oi.freight_value) AS revenue_price_freight,
  SUM(oi.freight_value) AS freight_total,
  COUNT(*) AS items_count
FROM olist_order_items_dataset_clean oi
GROUP BY oi.order_id;

CREATE INDEX idx_order_revenue_order_id ON order_revenue(order_id);

/* 3A.3 Orders fact (orders_enriched + order_revenue)
   Grain: one row per order
   Used as the base for KPIs and downstream marts.
*/
DROP TABLE IF EXISTS orders_fact;
CREATE TABLE orders_fact AS
SELECT
  e.order_id,
  e.customer_id,
  e.customer_unique_id,
  e.order_status,
  e.purchase_ts,
  e.approved_ts,
  e.carrier_ts,
  e.delivered_ts,
  e.estimated_delivery_ts,
  e.delivery_days,
  e.is_delayed,
  e.customer_city,
  e.customer_state,
  COALESCE(r.revenue_price, 0) AS revenue_price,
  COALESCE(r.revenue_price_freight, 0) AS revenue_price_freight,
  COALESCE(r.freight_total, 0) AS freight_total,
  COALESCE(r.items_count, 0) AS items_count
FROM orders_enriched e
LEFT JOIN order_revenue r
  ON e.order_id = r.order_id;

CREATE INDEX idx_orders_fact_purchase_ts ON orders_fact(purchase_ts);
CREATE INDEX idx_orders_fact_customer_unique ON orders_fact(customer_unique_id);
CREATE INDEX idx_orders_fact_status ON orders_fact(order_status);

/* Optional sanity checks */
-- SELECT COUNT(*) FROM olist_orders_dataset_clean;
-- SELECT COUNT(*) FROM orders_fact;
-- SELECT
--   SUM(revenue_price_freight) AS total_gmv_proxy,
--   SUM(revenue_price) AS total_price_only
-- FROM orders_fact
-- WHERE order_status = 'delivered';


/* ======================================================================
   STEP 3B — REVENUE VALIDATION (ITEMS VS PAYMENTS)
   Purpose: Validate the revenue definition used in dashboards.
   ====================================================================== */

/* 3B.1 Payments aggregated per order (payments can have multiple rows per order) */
DROP TABLE IF EXISTS order_payments_agg;
CREATE TABLE order_payments_agg AS
SELECT
  op.order_id,
  SUM(op.payment_value) AS payment_total,
  COUNT(*) AS payment_rows
FROM olist_order_payments_dataset_clean op
GROUP BY op.order_id;

CREATE INDEX idx_order_payments_agg_order_id ON order_payments_agg(order_id);

/* 3B.2 Reconciliation table
   Grain: one row per order
   Compares revenue proxies against payment totals.
*/
DROP TABLE IF EXISTS order_revenue_recon;
CREATE TABLE order_revenue_recon AS
SELECT
  o.order_id,
  o.order_status,
  o.purchase_ts,
  r.revenue_price,
  r.revenue_price_freight,
  pay.payment_total,
  (r.revenue_price - pay.payment_total) AS diff_price_vs_payment,
  (r.revenue_price_freight - pay.payment_total) AS diff_pricefreight_vs_payment,
  r.items_count,
  pay.payment_rows
FROM olist_orders_dataset_clean o
LEFT JOIN order_revenue r
  ON o.order_id = r.order_id
LEFT JOIN order_payments_agg pay
  ON o.order_id = pay.order_id;

CREATE INDEX idx_recon_status ON order_revenue_recon(order_status);
CREATE INDEX idx_recon_purchase_ts ON order_revenue_recon(purchase_ts);

/* 3B.3 Overall reconciliation (delivered orders only)
   Interpreting results:
   - avg_abs_diff_price: avg gap between price-only and payments (per order)
   - avg_abs_diff_price_freight: avg gap between (price+freight) and payments (per order)
   A smaller value indicates a better revenue proxy.
*/
SELECT
  COUNT(*) AS delivered_orders,
  SUM(revenue_price) AS sum_price,
  SUM(revenue_price_freight) AS sum_price_freight,
  SUM(payment_total) AS sum_payments,
  SUM(ABS(diff_price_vs_payment)) AS abs_diff_price,
  SUM(ABS(diff_pricefreight_vs_payment)) AS abs_diff_price_freight,
  AVG(ABS(diff_price_vs_payment)) AS avg_abs_diff_price,
  AVG(ABS(diff_pricefreight_vs_payment)) AS avg_abs_diff_price_freight
FROM order_revenue_recon
WHERE order_status = 'delivered';

/* 3B.4 Monthly reconciliation (delivered orders only)
   Use this to confirm stability of the chosen proxy over time.
*/
SELECT
  DATE_FORMAT(purchase_ts, '%Y-%m-01') AS month,
  COUNT(*) AS delivered_orders,
  SUM(revenue_price) AS sum_price,
  SUM(revenue_price_freight) AS sum_price_freight,
  SUM(payment_total) AS sum_payments,
  AVG(ABS(diff_price_vs_payment)) AS avg_abs_diff_price,
  AVG(ABS(diff_pricefreight_vs_payment)) AS avg_abs_diff_price_freight
FROM order_revenue_recon
WHERE order_status = 'delivered'
  AND purchase_ts IS NOT NULL
GROUP BY DATE_FORMAT(purchase_ts, '%Y-%m-01')
ORDER BY month;

/* 3B.5 Outliers (largest gaps between payments and chosen proxy)
   Use this to explain edge cases in documentation.
*/
SELECT
  order_id,
  order_status,
  purchase_ts,
  revenue_price,
  revenue_price_freight,
  payment_total,
  diff_price_vs_payment,
  diff_pricefreight_vs_payment,
  items_count,
  payment_rows
FROM order_revenue_recon
WHERE order_status = 'delivered'
  AND payment_total IS NOT NULL
ORDER BY ABS(diff_pricefreight_vs_payment) DESC
LIMIT 200;

/* 3B.6 Coverage check: delivered orders with payment data available */
SELECT
  DATE_FORMAT(purchase_ts, '%Y-%m-01') AS month,
  COUNT(*) AS delivered_orders,
  SUM(CASE WHEN payment_total IS NOT NULL THEN 1 ELSE 0 END) AS delivered_with_payments,
  ROUND(100.0 * SUM(CASE WHEN payment_total IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_payments
FROM order_revenue_recon
WHERE order_status = 'delivered'
  AND purchase_ts IS NOT NULL
GROUP BY DATE_FORMAT(purchase_ts, '%Y-%m-01')
ORDER BY month;
