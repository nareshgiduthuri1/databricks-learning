-- STEP 1: JSON -> Bronze Ingestion

-- Ingestion the JSON files from cloud storage into a streaming table using Auto Loader

CREATE OR REFRESH STREAMING TABLE db_retail.sdp_bronze.customers_bronze
COMMENT "Raw data from customers CDC feed"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed" = false
)
AS
SELECT
  *,
  current_timestamp() AS processing_time
FROM STREAM read_files(
  "/Volumes/db_retail/dbacademy_retail_v01/retail-pipeline/customers/", format => "json"
);



------------------------------------------------------------------------
-- STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement

-- STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement

CREATE STREAMING TABLE db_retail.sdp_bronze.customers_bronze_clean
(
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
  CONSTRAINT valid_address EXPECT (
    (address IS NOT NULL and
     city IS NOT NULL and
     state IS NOT NULL and
     zip_code IS NOT NULL) OR
     operation = "DELETE"),
  CONSTRAINT valid_email EXPECT (
    rlike(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,5}$') OR
     operation = "DELETE") ON VIOLATION DROP ROW
)
COMMENT "Clean raw bronze timestamp column and add data quality constraints"
AS
SELECT
  *,
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM db_retail.sdp_bronze.customers_bronze;

-- STEP 3: Processing CDC Data with AUTO CDC INTO


-- Create the streaming target table if it's not already created
CREATE OR REFRESH STREAMING TABLE db_retail.sdp_silver.customers_silver_scd_type_1
COMMENT 'SCD Type 1 Historical Customer Data';

-- Perform SCD TYPE 1
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO db_retail.sdp_silver.customers_silver_scd_type_1
FROM STREAM db_retail.sdp_bronze.customers_bronze_clean
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY timestamp_datetime
COLUMNS * EXCEPT (timestamp, _rescued_data, operation)
STORED AS SCD TYPE 1;
