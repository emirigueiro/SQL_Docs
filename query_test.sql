------------------------------------------------------------------
/* SUMMARY */
------------------------------------------------------------------
-- name: customer_churn_analysis
-- created_date: 2025-01-10
-- description: Identifies customers with high churn probability based on their last purchase date, activity frequency and product category. Used by the retention team to trigger re-engagement campaigns.
-- references: Ticket JIRA 821
------------------------------------------------------------------
/* RELATED PROGRAMS */
------------------------------------------------------------------
-- - campaign_trigger.py
-- - retention_dashboard.twbx
-- - customer_segmentation.sql
------------------------------------------------------------------
/* SOURCES */
------------------------------------------------------------------
-- - prod_bronze.crm.customers
-- - prod_bronze.sales.transactions
-- - prod_gold.marketing.campaign_history
-- - prod_silver.product.catalog
------------------------------------------------------------------
/* PRODUCTS */
------------------------------------------------------------------
-- - name: prod_gold.marketing.churn_candidates
--   type: table
--   description: Final table with customers flagged as high churn risk including their last activity date and assigned segment.
--   process: create or replace

-- - name: prod_gold.marketing.churn_summary
--   type: table
--   description: Aggregated summary of churn candidates by product category and customer segment used for executive reporting.
--   process: create or replace
------------------------------------------------------------------
/* HISTORICAL VERSIONS */
------------------------------------------------------------------
-- - date: 2025-01-10
--   user: john.doe
--   description: Initial creation of churn analysis query.

-- - date: 2025-02-03
--   user: jane.smith
--   description: Added product category filter and adjusted inactivity threshold from 60 to 45 days.

-- - date: 2025-03-15
--   user: john.doe
--   description: Included campaign_history source to exclude customers already contacted.
------------------------------------------------------------------
/* PROCESS COMMENTS */
------------------------------------------------------------------

-- STEP 1: Filter active customers from the last 12 months
WITH active_customers AS (
    SELECT
        customer_id,
        MAX(transaction_date) AS last_purchase_date,
        COUNT(*) AS total_transactions
    FROM prod_bronze.sales.transactions
    WHERE transaction_date >= DATEADD(MONTH, -12, CURRENT_DATE)
    GROUP BY customer_id
),

-- STEP 2: Calculate inactivity days and flag churn candidates
churn_flags AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.segment,
        ac.last_purchase_date,
        ac.total_transactions,
        DATEDIFF(DAY, ac.last_purchase_date, CURRENT_DATE) AS days_inactive,
        CASE
            WHEN DATEDIFF(DAY, ac.last_purchase_date, CURRENT_DATE) > 45 THEN 'High Risk'
            WHEN DATEDIFF(DAY, ac.last_purchase_date, CURRENT_DATE) > 30 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS churn_risk
    FROM prod_bronze.crm.customers c
    INNER JOIN active_customers ac ON c.customer_id = ac.customer_id
),

-- STEP 3: Exclude customers already contacted in active campaigns
filtered_churn AS (
    SELECT cf.*
    FROM churn_flags cf
    LEFT JOIN prod_gold.marketing.campaign_history ch
        ON cf.customer_id = ch.customer_id
        AND ch.campaign_status = 'active' -- NT: Only exclude customers in currently active campaigns, not historical ones
    WHERE ch.customer_id IS NULL
      AND cf.churn_risk IN ('High Risk', 'Medium Risk')
),

-- STEP 4: Enrich with product category of last purchase
enriched AS (
    SELECT
        fc.*,
        p.category AS last_product_category
    FROM filtered_churn fc
    LEFT JOIN prod_bronze.sales.transactions t
        ON fc.customer_id = t.customer_id
        AND t.transaction_date = fc.last_purchase_date -- NT: Join on last purchase date to get the most recent product category
    LEFT JOIN prod_silver.product.catalog p
        ON t.product_id = p.product_id
)

-- STEP 5: Final output - churn candidates table
SELECT
    customer_id,
    customer_name,
    segment,
    last_purchase_date,
    days_inactive,
    churn_risk,
    last_product_category,
    CURRENT_TIMESTAMP AS load_timestamp
FROM enriched;

-- STEP 6: Aggregated summary by category and segment
SELECT
    last_product_category,
    segment,
    churn_risk,
    COUNT(*) AS total_customers,
    ROUND(AVG(days_inactive), 1) AS avg_days_inactive
FROM enriched
GROUP BY last_product_category, segment, churn_risk
ORDER BY total_customers DESC;