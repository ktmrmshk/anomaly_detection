-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 1. オブジェクトストレージからログデータを読み込む

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transaction_readings_raw 
COMMENT "The raw transaction readings, ingested from /FileStore/tables/transaction_landing_dir" 
TBLPROPERTIES ("quality" = "bronze") 

AS
SELECT
  now() as ingest_time,
  *
FROM
  cloud_files(
    "/FileStore/tables/transaction_landing_dir",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  2. データのクレンジング

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transaction_readings_cleaned(
  CONSTRAINT valid_transaction_reading EXPECT (AMOUNT IS NOT NULL OR TIME IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Drop all rows with nulls for Time and store these records in a silver delta table"

AS
SELECT * FROM STREAM(live.transaction_readings_raw)
