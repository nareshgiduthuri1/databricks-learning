-- Create Table for bronze streaming table for json files in your source volume
CREATE OR REFRESH STREAMING TABLE db_retail.sdp_bronze.orders_bronze AS
select 
    *,
    current_timestamp() AS processing_time
    --,_metadata.file_name AS source_file 
FROM STREAM read_files(
    "/Volumes/db_retail/dbacademy_retail_v01/retail-pipeline/orders", format => 'JSON'
);

-- Create Silver Streaming Table with Bronze Streaming Table

CREATE OR REFRESH STREAMING TABLE db_retail.sdp_silver.orders_silver 
(
    CONSTRAINT valid_notifications EXPECT (notifications in ('Y', 'x')),
    CONSTRAINT valid_date EXPECT (order_timestamp > '2021-12-26') ON VIOLATION DROP ROW,
    CONSTRAINT valid_id EXPECT (customer_id is not null) on violation fail update
)
AS 
select
    order_id,
    timestamp(order_timestamp) as order_timestamp,
    customer_id,
    notifications
from stream db_retail.sdp_bronze.orders_bronze
;

-- create the gold materialized view aggregating the silver table with order date

CREATE OR REFRESH MATERIALIZED VIEW db_retail.sdp_gold.mv_gold_orders_by_date AS
select 
    date(order_timestamp) as order_date,
    count(*) as total_daily_orders
from db_retail.sdp_silver.orders_silver
group by date(order_timestamp)
;