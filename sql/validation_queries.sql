-- =============================================================================
-- Olist Project 3 — Athena Validation Queries
-- Database: olist_catalog
-- Region:   ca-central-1
-- =============================================================================
-- These queries were used to validate the star schema, diagnose data quality
-- issues, and derive the analytical findings documented in the README.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. SCHEMA VALIDATION
-- -----------------------------------------------------------------------------

-- Row counts across all tables
SELECT 'fact_orders'   AS tbl, COUNT(*) AS rows FROM olist_catalog.fact_orders
UNION ALL
SELECT 'dim_customer',          COUNT(*) FROM olist_catalog.dim_customer
UNION ALL
SELECT 'dim_product',           COUNT(*) FROM olist_catalog.dim_product
UNION ALL
SELECT 'dim_seller',            COUNT(*) FROM olist_catalog.dim_seller
UNION ALL
SELECT 'dim_date',              COUNT(*) FROM olist_catalog.dim_date
UNION ALL
SELECT 'dim_payment',           COUNT(*) FROM olist_catalog.dim_payment
UNION ALL
SELECT 'dim_geography',         COUNT(*) FROM olist_catalog.dim_geography;

-- Null key check on fact_orders
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN customer_key IS NULL THEN 1 END) AS null_customer_key,
    COUNT(CASE WHEN product_key  IS NULL THEN 1 END) AS null_product_key,
    COUNT(CASE WHEN seller_key   IS NULL THEN 1 END) AS null_seller_key,
    COUNT(CASE WHEN payment_key  IS NULL THEN 1 END) AS null_payment_key,
    COUNT(CASE WHEN date_key     IS NULL THEN 1 END) AS null_date_key
FROM olist_catalog.fact_orders;


-- -----------------------------------------------------------------------------
-- 2. ORDER STATUS DISTRIBUTION
-- -----------------------------------------------------------------------------

-- Used to decide whether order_status was worth visualizing (it wasn't)
SELECT order_status, COUNT(*) AS total
FROM olist_catalog.fact_orders
GROUP BY order_status
ORDER BY total DESC;


-- -----------------------------------------------------------------------------
-- 3. DELIVERY PERFORMANCE ANALYSIS
-- -----------------------------------------------------------------------------

-- delay_days distribution (note: negative = delivered before estimated date)
SELECT
    CASE
        WHEN delay_days <= 0              THEN 'On time or early'
        WHEN delay_days BETWEEN 1 AND 7   THEN '1-7 days late'
        WHEN delay_days BETWEEN 8 AND 14  THEN '8-14 days late'
        WHEN delay_days > 14              THEN '14+ days late'
    END AS delay_bucket,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM olist_catalog.fact_orders
WHERE order_status = 'delivered'
GROUP BY 1
ORDER BY 2 DESC;

-- Review score vs delivery time (core finding: ~0.25 score per 5 delivery days)
SELECT
    review_score,
    ROUND(AVG(delay_days), 2)        AS avg_delay,
    ROUND(AVG(days_to_deliver), 2)   AS avg_delivery_days,
    COUNT(*)                         AS total
FROM olist_catalog.fact_orders
WHERE order_status = 'delivered'
    AND review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

-- Delivery outliers — validate delay_days field integrity
SELECT
    COUNT(*)                                              AS total,
    COUNT(CASE WHEN delay_days > 30  THEN 1 END)         AS over_30_days_late,
    COUNT(CASE WHEN delay_days < -60 THEN 1 END)         AS over_60_days_early,
    COUNT(CASE WHEN days_to_deliver > 60 THEN 1 END)     AS delivery_over_60_days
FROM olist_catalog.fact_orders
WHERE order_status = 'delivered';


-- -----------------------------------------------------------------------------
-- 4. TEMPORAL ANALYSIS
-- -----------------------------------------------------------------------------

-- Orders and revenue by year
SELECT
    d.year,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    ROUND(SUM(f.total_value), 2)     AS total_revenue
FROM olist_catalog.fact_orders f
JOIN olist_catalog.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.year
ORDER BY d.year;

-- Monthly orders by year (used to identify data completeness and Black Friday spike)
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS orders
FROM olist_catalog.fact_orders f
JOIN olist_catalog.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
    AND d.year IN (2017, 2018)
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- YoY revenue growth — comparable periods only (Jan–Aug)
-- Note: 2018 data ends in August; full-year comparison would be misleading
SELECT
    SUM(CASE WHEN d.year = 2017 AND d.month <= 8 THEN f.total_value END) AS rev_2017_jan_aug,
    SUM(CASE WHEN d.year = 2018 AND d.month <= 8 THEN f.total_value END) AS rev_2018_jan_aug,
    ROUND(
        (SUM(CASE WHEN d.year = 2018 AND d.month <= 8 THEN f.total_value END) -
         SUM(CASE WHEN d.year = 2017 AND d.month <= 8 THEN f.total_value END)) * 100.0 /
         SUM(CASE WHEN d.year = 2017 AND d.month <= 8 THEN f.total_value END)
    , 2) AS yoy_growth_pct
FROM olist_catalog.fact_orders f
JOIN olist_catalog.dim_date d ON f.date_key = d.date_key;


-- -----------------------------------------------------------------------------
-- 5. SELLER ANALYSIS
-- -----------------------------------------------------------------------------

-- Top 10 sellers by revenue with satisfaction metrics
SELECT
    s.seller_id,
    s.seller_state,
    COUNT(DISTINCT f.order_id)           AS total_orders,
    ROUND(SUM(f.total_value), 2)         AS total_revenue,
    ROUND(AVG(f.review_score), 2)        AS avg_review_score,
    ROUND(AVG(f.days_to_deliver), 2)     AS avg_delivery_days
FROM olist_catalog.fact_orders f
JOIN olist_catalog.dim_seller s ON f.seller_key = s.seller_key
WHERE f.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_state
ORDER BY total_revenue DESC
LIMIT 10;

-- Sellers with poor satisfaction (50+ orders, lowest review scores)
SELECT
    ROUND(AVG(f.days_to_deliver), 2)     AS avg_delivery_days,
    ROUND(AVG(f.review_score), 2)        AS avg_review_score,
    COUNT(DISTINCT f.order_id)           AS total_orders,
    s.seller_state
FROM olist_catalog.fact_orders f
JOIN olist_catalog.dim_seller s ON f.seller_key = s.seller_key
WHERE f.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_state
HAVING COUNT(DISTINCT f.order_id) >= 50
ORDER BY avg_review_score ASC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 6. CUSTOMER SEGMENTATION
-- -----------------------------------------------------------------------------

-- Dataset date range (used to anchor segmentation thresholds)
SELECT
    MAX(date) AS max_date,
    MIN(date) AS min_date
FROM olist_catalog.dim_date
WHERE date_key IN (
    SELECT DISTINCT date_key
    FROM olist_catalog.fact_orders
    WHERE order_status = 'delivered'
);

-- Customer segment distribution
-- Reference date: 2018-08-29 (dataset max date)
-- New:      1 order, last purchase >= 2018-05-31 (last 90 days)
-- Returning: 2+ orders
-- Dormant:  1 order, last purchase 2017-08-29 to 2018-05-30 (90 days–1 year)
-- Churned:  1 order, last purchase < 2017-08-29 (1+ year ago)
WITH customer_stats AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT f.order_id)   AS total_orders,
        MAX(d.date)                  AS last_order_date,
        MIN(d.date)                  AS first_order_date
    FROM olist_catalog.fact_orders f
    JOIN olist_catalog.dim_customer c ON f.customer_key = c.customer_key
    JOIN olist_catalog.dim_date d     ON f.date_key = d.date_key
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)
SELECT
    CASE
        WHEN total_orders >= 2
             AND last_order_date < DATE '2018-02-28' THEN 'Churned (was Returning)'
        WHEN total_orders >= 2                       THEN 'Returning'
        WHEN total_orders = 1
             AND last_order_date >= DATE '2018-05-31' THEN 'New'
        WHEN total_orders = 1
             AND last_order_date >= DATE '2017-08-29' THEN 'Dormant'
        ELSE 'Churned'
    END AS segment,
    COUNT(*)                                              AS total_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)    AS pct
FROM customer_stats
GROUP BY 1
ORDER BY total_customers DESC;

-- First-time buyer acquisition curve by month
WITH first_purchases AS (
    SELECT
        c.customer_unique_id,
        MIN(d.date) AS first_purchase_date
    FROM olist_catalog.fact_orders f
    JOIN olist_catalog.dim_customer c ON f.customer_key = c.customer_key
    JOIN olist_catalog.dim_date d     ON f.date_key = d.date_key
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)
SELECT
    SUBSTR(CAST(first_purchase_date AS VARCHAR), 1, 7) AS first_purchase_month,
    COUNT(*)                                           AS new_customers
FROM first_purchases
WHERE first_purchase_date >= DATE '2017-01-01'
GROUP BY SUBSTR(CAST(first_purchase_date AS VARCHAR), 1, 7)
ORDER BY first_purchase_month;
