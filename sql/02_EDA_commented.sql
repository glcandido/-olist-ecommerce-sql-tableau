/****************************************************************************************
 * EDA.sql (Commented / Proofed)
 * Project: Olist E-commerce Growth & Retention Analytics (MySQL)
 *
 * Step 3 Objective
 *   Create analysis-ready "gold" tables for Tableau/BI. These tables are derived from the
 *   Step 2 cleaned tables (suffix *_clean) and are designed to be stable, re-runnable, and
 *   easy to explain in a portfolio.
 *
 * Step 2 Dependencies (must already exist)
 *   - olist_orders_dataset_clean
 *   - olist_customers_dataset_clean
 *   - olist_order_items_dataset_clean
 *   - olist_order_payments_dataset_clean
 *   - olist_order_reviews_dataset_clean
 *   - olist_products_dataset_clean
 *
 * Revenue Standard (validated)
 *   - Revenue (GMV proxy) = SUM(price + freight_value) from order items.
 *
 * Notes
 *   - CTAS (CREATE TABLE ... AS SELECT) does not carry keys/constraints; this script
 *     explicitly normalizes key column types before creating indexes.
 ****************************************************************************************/

/* --------------------------------------------------------------------------
   SECTION 1: Orders + Customers (enriched order grain)

   What this does:
     Creates an order-grain table by joining cleaned orders to cleaned customers so that
     each order has customer identity (including customer_unique_id) and location fields.

   Grain:
     1 row per order_id.

   Output:
     customers_orders_enriched
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS customers_orders_enriched;

CREATE TABLE customers_orders_enriched AS
SELECT
  o.*,  -- includes purchase_ts, delivered_ts, delivery_days, is_delayed, etc.
  c.customer_unique_id,
  c.customer_city,
  c.customer_state
FROM olist_orders_dataset_clean o
JOIN olist_customers_dataset_clean c
  ON o.customer_id = c.customer_id;

-- Normalize common key column types (helps indexing and join performance).
ALTER TABLE customers_orders_enriched
  MODIFY order_id CHAR(32) NOT NULL,
  MODIFY customer_id CHAR(32) NOT NULL,
  MODIFY customer_unique_id CHAR(32) NOT NULL,
  MODIFY order_status VARCHAR(20) NULL;

CREATE INDEX idx_coe_order_id      ON customers_orders_enriched(order_id);
CREATE INDEX idx_coe_customer_id   ON customers_orders_enriched(customer_id);
CREATE INDEX idx_coe_customer_uniq ON customers_orders_enriched(customer_unique_id);
CREATE INDEX idx_coe_purchase_ts   ON customers_orders_enriched(purchase_ts);
CREATE INDEX idx_coe_status        ON customers_orders_enriched(order_status);

-- Optional spot check
-- SELECT * FROM customers_orders_enriched LIMIT 25;


/* --------------------------------------------------------------------------
   SECTION 2: Order Revenue (aggregate items to order grain)

   What this does:
     Aggregates cleaned order items to compute order-level revenue metrics.

   Grain:
     1 row per order_id.

   Output:
     order_revenue_enriched

   Revenue fields:
     revenue_gmv     = SUM(price + freight_value)
     revenue_price   = SUM(price)
     freight_total   = SUM(freight_value)
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS order_revenue_enriched;

CREATE TABLE order_revenue_enriched AS
SELECT
  oi.order_id,
  SUM(oi.price + oi.freight_value) AS revenue_gmv,
  SUM(oi.price)                   AS revenue_price,
  SUM(oi.freight_value)           AS freight_total,
  COUNT(*)                        AS items_count
FROM olist_order_items_dataset_clean oi
GROUP BY oi.order_id;

ALTER TABLE order_revenue_enriched
  MODIFY order_id CHAR(32) NOT NULL;

CREATE INDEX idx_ore_order_id ON order_revenue_enriched(order_id);

-- Optional spot check
-- SELECT * FROM order_revenue_enriched LIMIT 25;


/* --------------------------------------------------------------------------
   SECTION 3: Fact Table (order grain with revenue attached)

   What this does:
     Creates a single order-grain fact table that is the primary source for KPI and
     retention marts. Combines:
       customers_orders_enriched  (order + customer identity)
       order_revenue_enriched     (order-level revenue metrics)

   Grain:
     1 row per order_id.

   Output:
     fact_orders_enriched
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS fact_orders_enriched;

CREATE TABLE fact_orders_enriched AS
SELECT
  e.order_id,
  e.customer_unique_id,
  e.customer_id,
  e.order_status,
  e.purchase_ts,
  e.delivered_ts,
  e.estimated_delivery_ts,
  e.delivery_days,
  e.is_delayed,
  e.customer_city,
  e.customer_state,
  COALESCE(r.revenue_gmv, 0)     AS revenue_gmv,
  COALESCE(r.revenue_price, 0)   AS revenue_price,
  COALESCE(r.freight_total, 0)   AS freight_total,
  COALESCE(r.items_count, 0)     AS items_count
FROM customers_orders_enriched e
LEFT JOIN order_revenue_enriched r
  ON e.order_id = r.order_id;

ALTER TABLE fact_orders_enriched
  MODIFY order_id CHAR(32) NOT NULL,
  MODIFY customer_unique_id CHAR(32) NOT NULL,
  MODIFY customer_id CHAR(32) NOT NULL,
  MODIFY order_status VARCHAR(20) NULL;

CREATE INDEX idx_foe_purchase_ts    ON fact_orders_enriched(purchase_ts);
CREATE INDEX idx_foe_customer_uniq  ON fact_orders_enriched(customer_unique_id);
CREATE INDEX idx_foe_status         ON fact_orders_enriched(order_status);

-- Optional spot check
-- SELECT * FROM fact_orders_enriched LIMIT 25;


/* --------------------------------------------------------------------------
   SECTION 4: Monthly KPI Mart (Executive Overview)

   What this does:
     Creates a monthly table of headline KPIs for Tableau.

   Grain:
     1 row per month.

   Output:
     mart_monthly_kpis

   Important filter:
     Uses only delivered orders for realized revenue and delivery performance.
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mart_monthly_kpis;

CREATE TABLE mart_monthly_kpis AS
SELECT
  DATE_FORMAT(purchase_ts, '%Y-%m-01')     AS month,
  COUNT(DISTINCT order_id)                AS orders,
  COUNT(DISTINCT customer_unique_id)      AS customers,
  SUM(revenue_gmv)                        AS revenue_gmv,
  AVG(revenue_gmv)                        AS avg_gmv,
  SUM(items_count)                        AS items_sold,
  SUM(items_count) / COUNT(DISTINCT order_id) AS items_per_order,
  AVG(delivery_days)                      AS avg_delivery_days,
  AVG(is_delayed)                         AS delay_rate
FROM fact_orders_enriched
WHERE order_status = 'delivered'
  AND purchase_ts IS NOT NULL
GROUP BY DATE_FORMAT(purchase_ts, '%Y-%m-01');

ALTER TABLE mart_monthly_kpis
  MODIFY month DATE NOT NULL;

CREATE INDEX idx_mmk_month ON mart_monthly_kpis(month);

-- Optional spot check
-- SELECT * FROM mart_monthly_kpis ORDER BY month;


/* --------------------------------------------------------------------------
   SECTION 5 (Optional / Advanced): Cohort Retention Mart

   What this does:
     Builds a cohort table (cohort_month x months_since_cohort) with active customer counts.
     This is valuable for a portfolio, but you can skip it if you prefer a simpler scope.

   Grain:
     cohort_month x months_since_cohort.

   Output:
     mart_cohorts
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mart_cohorts;

CREATE TABLE mart_cohorts AS
WITH delivered AS (
  SELECT
    customer_unique_id,
    order_id,
    purchase_ts,
    DATE_FORMAT(purchase_ts, '%Y-%m-01') AS order_month
  FROM fact_orders_enriched
  WHERE order_status = 'delivered'
    AND purchase_ts IS NOT NULL
),
first_purchase AS (
  SELECT
    customer_unique_id,
    MIN(order_month) AS cohort_month
  FROM delivered
  GROUP BY customer_unique_id
),
activity AS (
  SELECT
    d.customer_unique_id,
    f.cohort_month,
    d.order_month,
    TIMESTAMPDIFF(MONTH, f.cohort_month, d.order_month) AS months_since_cohort
  FROM delivered d
  JOIN first_purchase f
    ON d.customer_unique_id = f.customer_unique_id
)
SELECT
  cohort_month,
  months_since_cohort,
  COUNT(DISTINCT customer_unique_id) AS active_customers
FROM activity
GROUP BY cohort_month, months_since_cohort;

ALTER TABLE mart_cohorts
  MODIFY cohort_month DATE NOT NULL,
  MODIFY months_since_cohort INT NOT NULL;

CREATE INDEX idx_mc_cohort ON mart_cohorts(cohort_month, months_since_cohort);

-- Optional spot check
-- SELECT * FROM mart_cohorts ORDER BY cohort_month, months_since_cohort LIMIT 100;


/* --------------------------------------------------------------------------
   SECTION 6 (Optional / Advanced): RFM Base Mart

   What this does:
     Builds Recency/Frequency/Monetary metrics per customer (delivered only).
     You can stop at this table (no scoring) to keep the project lighter.

   Grain:
     1 row per customer_unique_id.

   Output:
     mart_customer_rfm
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mart_customer_rfm;

CREATE TABLE mart_customer_rfm AS
WITH delivered AS (
  SELECT *
  FROM fact_orders_enriched
  WHERE order_status = 'delivered'
    AND purchase_ts IS NOT NULL
),
asof AS (
  SELECT MAX(purchase_ts) AS asof_date FROM delivered
),
agg AS (
  SELECT
    customer_unique_id,
    MAX(purchase_ts) AS last_purchase_ts,
    COUNT(DISTINCT order_id) AS frequency_orders,
    SUM(revenue_gmv) AS monetary_gmv
  FROM delivered
  GROUP BY customer_unique_id
)
SELECT
  a.customer_unique_id,
  TIMESTAMPDIFF(DAY, a.last_purchase_ts, (SELECT asof_date FROM asof)) AS recency_days,
  a.frequency_orders,
  a.monetary_gmv
FROM agg a;

ALTER TABLE mart_customer_rfm
  MODIFY customer_unique_id CHAR(32) NOT NULL;

CREATE INDEX idx_rfm_customer ON mart_customer_rfm(customer_unique_id);

-- Optional profiling of RFM ranges
-- SELECT
--   COUNT(*) AS customers,
--   MIN(recency_days) AS min_recency,
--   MAX(recency_days) AS max_recency,
--   MIN(frequency_orders) AS min_freq,
--   MAX(frequency_orders) AS max_freq
-- FROM mart_customer_rfm;


/* --------------------------------------------------------------------------
   SECTION 7: Ops Driver Mart (Delivery vs Reviews)

   What this does:
     Creates a joinable dataset to analyze delivery performance and customer review scores.

   Grain:
     1 row per (order_id) with review fields nullable (LEFT JOIN).

   Output:
     mart_delivery_reviews
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mart_delivery_reviews;

CREATE TABLE mart_delivery_reviews AS
SELECT
  f.order_id,
  f.customer_unique_id,
  f.purchase_ts,
  f.delivery_days,
  f.is_delayed,
  r.review_score,
  r.review_creation_ts
FROM fact_orders_enriched f
LEFT JOIN olist_order_reviews_dataset_clean r
  ON f.order_id = r.order_id
WHERE f.order_status = 'delivered';

ALTER TABLE mart_delivery_reviews
  MODIFY order_id CHAR(32) NOT NULL,
  MODIFY customer_unique_id CHAR(32) NOT NULL;

CREATE INDEX idx_mdr_order_id ON mart_delivery_reviews(order_id);
CREATE INDEX idx_mdr_delayed ON mart_delivery_reviews(is_delayed);
CREATE INDEX idx_mdr_score   ON mart_delivery_reviews(review_score);


/* --------------------------------------------------------------------------
   SECTION 8: Sanity Checks (fast validation)

   What this does:
     Quick checks to ensure the fact table is usable for downstream marts.
-------------------------------------------------------------------------- */

-- Delivered share + revenue sanity
SELECT
  SUM(order_status = 'delivered') AS delivered_orders,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN order_status = 'delivered' THEN revenue_gmv ELSE 0 END) AS delivered_revenue_gmv
FROM fact_orders_enriched;

-- Missing customer_unique_id validation (should be 0)
SELECT COUNT(*) AS missing_customer_unique_id
FROM fact_orders_enriched
WHERE customer_unique_id IS NULL OR customer_unique_id = '';


/* --------------------------------------------------------------------------
   SECTION 9: Retention (simple, portfolio-friendly)

   What this does:
     Produces a minimal retention view:
       - overall repeat customer rate
       - monthly new vs repeat customers

   Outputs:
     customer_order_counts
     customer_first_month
     mart_monthly_new_repeat
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS customer_order_counts;

CREATE TABLE customer_order_counts AS
SELECT
  customer_unique_id,
  COUNT(DISTINCT order_id) AS delivered_orders
FROM fact_orders_enriched
WHERE order_status = 'delivered'
GROUP BY customer_unique_id;

ALTER TABLE customer_order_counts
  MODIFY customer_unique_id CHAR(32) NOT NULL;

CREATE INDEX idx_coc_customer ON customer_order_counts(customer_unique_id);

-- Overall repeat rate (>= 2 delivered orders)
SELECT
  COUNT(*) AS customers,
  SUM(delivered_orders >= 2) AS repeat_customers,
  ROUND(SUM(delivered_orders >= 2) / COUNT(*) * 100, 2) AS repeat_rate_pct
FROM customer_order_counts;


DROP TABLE IF EXISTS customer_first_month;

CREATE TABLE customer_first_month AS
SELECT
  customer_unique_id,
  MIN(DATE_FORMAT(purchase_ts, '%Y-%m-01')) AS first_month
FROM fact_orders_enriched
WHERE order_status = 'delivered'
  AND purchase_ts IS NOT NULL
GROUP BY customer_unique_id;

ALTER TABLE customer_first_month
  MODIFY customer_unique_id CHAR(32) NOT NULL,
  MODIFY first_month DATE NOT NULL;

CREATE INDEX idx_cfm_customer_month ON customer_first_month(customer_unique_id, first_month);


DROP TABLE IF EXISTS mart_monthly_new_repeat;

CREATE TABLE mart_monthly_new_repeat AS
WITH delivered_orders AS (
  SELECT
    customer_unique_id,
    purchase_ts,
    DATE_FORMAT(purchase_ts, '%Y-%m-01') AS order_month
  FROM fact_orders_enriched
  WHERE order_status = 'delivered'
    AND purchase_ts IS NOT NULL
),
first_month AS (
  SELECT
    customer_unique_id,
    MIN(order_month) AS first_month
  FROM delivered_orders
  GROUP BY customer_unique_id
)
SELECT
  d.order_month AS month,
  COUNT(DISTINCT d.customer_unique_id) AS customers,
  COUNT(DISTINCT CASE WHEN f.first_month = d.order_month THEN d.customer_unique_id END) AS new_customers,
  COUNT(DISTINCT CASE WHEN f.first_month < d.order_month THEN d.customer_unique_id END) AS repeat_customers
FROM delivered_orders d
JOIN first_month f
  ON d.customer_unique_id = f.customer_unique_id
GROUP BY d.order_month
ORDER BY d.order_month;

CREATE INDEX idx_mnr_month ON mart_monthly_new_repeat(month);

-- Optional spot check
-- SELECT * FROM mart_monthly_new_repeat ORDER BY month;


/* --------------------------------------------------------------------------
   SECTION 10: Commercial Insights (categories + top products)

   What this does:
     Creates category and product ranking tables using the GMV proxy metric.

   Outputs:
     mart_category_revenue
     mart_top_products
-------------------------------------------------------------------------- */

DROP TABLE IF EXISTS mart_category_revenue;

CREATE TABLE mart_category_revenue AS
SELECT
  COALESCE(NULLIF(TRIM(p.category_english), ''), 'Unknown') AS category,
  SUM(oi.price + oi.freight_value) AS revenue_gmv,
  COUNT(DISTINCT oi.order_id) AS orders,
  COUNT(*) AS items_sold
FROM olist_order_items_dataset_clean oi
JOIN fact_orders_enriched f
  ON f.order_id = oi.order_id
LEFT JOIN olist_products_dataset_clean p
  ON p.product_id = oi.product_id
WHERE f.order_status = 'delivered'
GROUP BY COALESCE(NULLIF(TRIM(p.category_english), ''), 'Unknown');

-- Ensure category is index-friendly (avoid TEXT prefix indexes).
ALTER TABLE mart_category_revenue
  MODIFY category VARCHAR(120) NOT NULL;

CREATE INDEX idx_mcr_category ON mart_category_revenue(category);

-- Optional: true Top 5 by revenue
-- SELECT category, revenue_gmv, orders, items_sold
-- FROM mart_category_revenue
-- ORDER BY revenue_gmv DESC
-- LIMIT 5;


DROP TABLE IF EXISTS mart_top_products;

CREATE TABLE mart_top_products AS
SELECT
  oi.product_id,
  COALESCE(NULLIF(TRIM(p.category_english), ''), 'Unknown') AS category,
  SUM(oi.price + oi.freight_value) AS revenue_gmv,
  COUNT(*) AS items_sold,
  COUNT(DISTINCT oi.order_id) AS orders
FROM olist_order_items_dataset_clean oi
JOIN fact_orders_enriched f
  ON f.order_id = oi.order_id
LEFT JOIN olist_products_dataset_clean p
  ON p.product_id = oi.product_id
WHERE f.order_status = 'delivered'
GROUP BY oi.product_id, COALESCE(NULLIF(TRIM(p.category_english), ''), 'Unknown')
ORDER BY revenue_gmv DESC
LIMIT 20;

ALTER TABLE mart_top_products
  MODIFY product_id CHAR(32) NOT NULL,
  MODIFY category VARCHAR(120) NOT NULL;

CREATE INDEX idx_mtp_product_id ON mart_top_products(product_id);
CREATE INDEX idx_mtp_category   ON mart_top_products(category);

-- Optional spot check
-- SELECT * FROM mart_top_products;

SELECT *
FROM mart_category_revenue
ORDER BY revenue_gmv DESC
LIMIT 10;

SELECT *
FROM mart_monthly_new_repeat
ORDER BY month;

-- total GMV proxy
SELECT SUM(revenue_gmv) FROM mart_category_revenue;

-- Top 5 GMV proxy
SELECT SUM(revenue_gmv) FROM (SELECT revenue_gmv FROM mart_category_revenue ORDER BY revenue_gmv DESC LIMIT 5) x;
