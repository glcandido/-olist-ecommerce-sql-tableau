### DATA CLEANING E-commerce Growth & Retention Analytics (Olist): Revenue, Cohorts, RFM Segmentation, and Delivery Performance

## Identifying inconsistencies to be fixed.

-- STEP 1 (ROW COUNT)

SELECT 'orders' AS tbl, COUNT(*) FROM olist_orders_dataset
UNION ALL SELECT 'items', COUNT(*) FROM olist_order_items_dataset
UNION ALL SELECT 'payments', COUNT(*) FROM olist_order_payments_dataset
UNION ALL SELECT 'customers', COUNT(*) FROM olist_customers_dataset
UNION ALL SELECT 'products', COUNT(*) FROM olist_products_dataset
UNION ALL SELECT 'reviews', COUNT(*) FROM olist_order_reviews_dataset;

-- STEP 2 (KEY UNIQUENESS CHECK)

-- order_id should be unique
SELECT COUNT(*) AS orders,
COUNT(DISTINCT order_id) AS distinct_order_id
FROM olist_orders_dataset;

-- reviews: order_id may have multiple rows; check review_id uniqueness
SELECT COUNT(*) AS reviews,
COUNT(DISTINCT review_id) AS distinct_review_id
FROM olist_order_reviews_dataset;


-- STEP 3: Missing on critical fields
SELECT 
	SUM(CASE WHEN order_purchase_timestamp IS NULL OR order_purchase_timestamp = '' THEN 1 ELSE 0 END) AS missing_purchase_ts,
	SUM(CASE WHEN order_delivered_customer_date IS NULL OR order_delivered_customer_date = '' THEN 1 ELSE 0 END) AS missing_order_delivery_date,
    SUM(CASE WHEN order_status IS NULL OR order_status = '' THEN 1 ELSE 0 END) AS missing_status
FROM olist_orders_dataset;

-- STEP 4: Order status distribution
SELECT order_status, COUNT(*)
FROM olist_orders_dataset
GROUP BY order_status
ORDER BY COUNT(*) DESC;


