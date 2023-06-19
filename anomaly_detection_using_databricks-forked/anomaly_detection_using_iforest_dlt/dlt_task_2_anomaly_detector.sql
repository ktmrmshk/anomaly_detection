-- Databricks notebook source
-- MAGIC %md ### Delta Live Tables - 異常検知モデルの適用

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE predictions
COMMENT "Use the DeepSVDD pandas udf registered in the previous step to predict anomalous transaction readings"
TBLPROPERTIES ("quality" = "gold")
AS SELECT cust_id, detect_anomaly(named_struct("Time", Time,  "V1", V1,  "V2", V2,  "V3", V3,  "V4", V4,  "V5", V5,  "V6", V6,  "V7", V7,  "V8", V8,  "V9", V9,  "V10", V10,  "V11", V11,  "V12", V12,  "V13", V13,  "V14", V14,  "V15", V15,  "V16", V16,  "V17", V17,  "V18", V18,  "V19", V19,  "V20", V20,  "V21", V21,  "V22", V22,  "V23", V23,  "V24", V24,  "V25", V25,  "V26", V26,  "V27", V27,  "V28", V28, "Amount", Amount )) as is_anomaly from STREAM(live.transaction_readings_cleaned)
