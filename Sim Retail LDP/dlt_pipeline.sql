-- should inherit from pipeline config
USE CATALOG ${config_catalog};
USE SCHEMA ${config_schema};

-- bronze streaming table queries

CREATE OR REPLACE STREAMING TABLE customer_bronze_ldp
AS
SELECT *,
  current_timestamp() as processing_time,
  _metadata.file_name
FROM STREAM read_files(
  '/Volumes/${config_catalog}/${config_schema}/customer',
  format => 'json',
  inferSchema => true
);

CREATE OR REPLACE STREAMING TABLE orders_bronze_ldp
AS
SELECT *,
  current_timestamp() as processing_time,
  _metadata.file_name
FROM STREAM read_files(
  '/Volumes/${config_catalog}/${config_schema}/orders',
  format => 'json',
  inferSchema => true
);

CREATE OR REPLACE STREAMING TABLE status_bronze_ldp
AS
SELECT *,
  current_timestamp() as processing_time,
  _metadata.file_name
FROM STREAM read_files(
  '/Volumes/${config_catalog}/${config_schema}/status',
  format => 'json',
  inferSchema => true
);

-- silver streaming table queries

CREATE OR REPLACE STREAMING TABLE customer_bronze_clean_ldp
AS
SELECT 
  *,
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM customer_bronze_ldp
WHERE operation = 'NEW';

-- USE automatic cdc via scd type 1 to maintain current customer records instead of merge into call in other pipeline
CREATE OR REFRESH STREAMING TABLE customer_silver_ldp;

CREATE FLOW scd_type_1_flow AS 
AUTO CDC INTO customer_silver_ldp 
FROM STREAM customer_bronze_clean_ldp  
  KEYS (customer_id)                              
  APPLY AS DELETE WHEN operation = "DELETE"      
  SEQUENCE BY timestamp_datetime                 
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation)   
  STORED AS SCD TYPE 1;   

CREATE OR REPLACE STREAMING TABLE orders_silver_ldp
AS
SELECT 
  order_id,
  customer_id,
  to_timestamp(order_timestamp) as order_timestamp,
  notifications
FROM STREAM orders_bronze_ldp;

CREATE OR REPLACE STREAMING TABLE status_silver_ldp
AS
SELECT 
  order_id,
  order_status,
  to_timestamp(status_timestamp) as status_timestamp
FROM STREAM status_bronze_ldp;

-- gold materialized view queries

CREATE OR REPLACE MATERIALIZED VIEW gold_orders_by_city_state_ldp
AS
SELECT
  customers.city,
  customers.state,
  COUNT(orders.order_id) as total_orders
FROM orders_silver_ldp orders
JOIN customer_silver_ldp customers
ON orders.customer_id = customers.customer_id
GROUP BY customers.city, customers.state
ORDER BY total_orders DESC;

CREATE OR REPLACE MATERIALIZED VIEW gold_returns_by_customer_ldp
AS

WITH returned_orders 
AS (
  SELECT DISTINCT order_id
  FROM status_silver_ldp
  WHERE order_status IN ("return requested", "return picked up", "return processed")
),

order_customer_ids
AS (
  SELECT 
    order_id,
    customer_id
  FROM orders_silver_ldp
)

SELECT 
  customers.customer_id as customer_id,
  customers.city as city,
  customers.state as state,
  COUNT(returned.order_id) as total_returns
FROM returned_orders returned
JOIN order_customer_ids ids
ON returned.order_id = ids.order_id
JOIN customer_silver_ldp customers
ON ids.customer_id = customers.customer_id
GROUP BY customers.customer_id, customers.city, customers.state
ORDER BY total_returns DESC;

CREATE OR REPLACE MATERIALIZED VIEW gold_customer_order_history_ldp
AS
SELECT 
  customers.customer_id as customer_id,
  orders.order_id as order_id,
  status.order_status as order_status,
  status.status_timestamp as status_update_time
FROM orders_silver_ldp orders
JOIN status_silver_ldp status
ON orders.order_id = status.order_id
JOIN customer_silver_ldp customers
ON orders.customer_id = customers.customer_id;


CREATE OR REFRESH MATERIALIZED VIEW gold_current_orders_ldp
AS

WITH status_ranked AS (
  SELECT 
    s.*,
    r.rank,
    row_number() OVER (PARTITION BY s.order_id ORDER BY r.rank DESC) AS current_status
  FROM status_silver_ldp s
  LEFT JOIN order_status_ranking r
  ON s.order_status = r.order_status
)

SELECT 
  sr.order_id,
  o.customer_id,
  sr.order_status as current_order_status,
  sr.status_timestamp as current_status_timestamp,
  o.order_timestamp as order_placed_timestamp
FROM status_ranked sr
LEFT JOIN orders_silver_ldp o 
ON sr.order_id = o.order_id
WHERE sr.current_status = 1;