-- Register Delta tables as session_products, sessions, product_metrics before running.
-- Local: spark.read.format('delta').load('output/gold/<table>').createOrReplaceTempView('<table>')
-- Databricks: CREATE TEMP VIEW sessions USING DELTA LOCATION '<lakehouse>/gold/sessions';

-- Customers returning for more than one session and their interaction totals.
SELECT user_id, COUNT(*) AS session_count,
       SUM(event_count) AS total_events, SUM(purchases) AS purchase_events,
       MIN(session_start) AS first_seen, MAX(session_end) AS last_seen
FROM sessions
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY session_count DESC, user_id;

-- Product funnel performance. NULL rates indicate no eligible denominator.
SELECT product_id, interacting_sessions, viewed_sessions, carted_sessions, converted_sessions,
       CASE WHEN viewed_sessions > 0
            THEN converted_sessions * 1.0 / viewed_sessions ELSE NULL END AS conversion_rate,
       purchase_events
FROM product_metrics
ORDER BY converted_sessions DESC, product_id;

-- Product-level cart abandonment among ordered view -> cart journeys.
SELECT product_id,
       COUNT(first_cart_after_view) AS carted_sessions,
       COUNT(first_cart_after_view) - COUNT(first_purchase_after_cart) AS abandoned_sessions,
       CASE WHEN COUNT(first_cart_after_view) > 0 THEN
         1.0 - COUNT(first_purchase_after_cart) * 1.0 / COUNT(first_cart_after_view)
       ELSE NULL END AS abandonment_rate
FROM session_products
GROUP BY product_id
ORDER BY abandoned_sessions DESC, product_id;
