
-- Create streaming table for status bronze streaming table for json files in your source volume

CREATE OR REFRESH STREAMING TABLE db_retail.sdp_bronze.status_bronze
  COMMENT "Ingest raw JSON order status files from cloud storage"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false  -- prevent full table refreshes on the bronze table
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time
  --_metadata.file_name AS source_file
FROM STREAM read_files(
  "/Volumes/db_retail/dbacademy_retail_v01/retail-pipeline/status", format => "json"
);

----------------------------------------------------------------------------
-- We're also adding a comment and table properties to document the table for production use,
-- along with pipeline expectations to enforce data quality on the streaming table.
----------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE db_retail.sdp_silver.status_silver
(
  -- Drop rows if order_status_timestamp is not valid
  CONSTRAINT valid_timestamp EXPECT (order_status_timestamp > "2021-12-25") ON VIOLATION DROP ROW,
    
  -- Warn if order_status is not in the following
  CONSTRAINT valid_order_status
    EXPECT (
      order_status IN (
        'on the way',
        'canceled',
        'return canceled',
        'delivered',
        'return processed',
        'placed',
        'preparing'
      )
    )
)
COMMENT "Order with each status and timestamp"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  order_id,
  order_status,
  timestamp(status_timestamp) AS order_status_timestamp
FROM STREAM db_retail.sdp_bronze.status_bronze;

-- Creating and writing silver data to gold table

CREATE OR REFRESH MATERIALIZED VIEW db_retail.sdp_gold.mv_gold_status_order_info
COMMENT "Joining the orders and order status silver tables to view all orders with each individual status per order"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  orders.order_id,
  orders.order_timestamp,
  status.order_status,
  status.order_status_timestamp
-- Notice that the STREAM keyword was not used when referencing the streaming tables to create the MV
FROM db_retail.sdp_silver.status_silver status
INNER JOIN db_retail.sdp_silver.orders_silver orders
  ON orders.order_id = status.order_id;


-----------------------------------------------------------------------
-- D. Create Gold Materialized Views for Cancelled and Delivered Orders
-- The code below will create two gold materialized views using the joined data from above (orders and status):

-- CANCELLED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW db_retail.sdp_gold.cancelled_orders_gold
COMMENT "All cancelled orders"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY, order_timestamp, order_status_timestamp) AS days_to_cancel -- calculate days to cancel
FROM db_retail.sdp_gold.mv_gold_status_order_info
WHERE order_status = 'canceled';

-- DELIVERED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW db_retail.sdp_gold.delivered_orders_gold
COMMENT "All delivered orders"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY, order_timestamp, order_status_timestamp) AS days_to_delivery -- calculate days to deliver
FROM db_retail.sdp_gold.mv_gold_status_order_info
WHERE order_status = 'delivered';