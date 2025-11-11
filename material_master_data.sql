-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE mm_bronze
TBLPROPERTIES (
  "checkpointLocation" = "/Volumes/workspace/damg7370/datastore/checkpoint/bronze",
  "delta.columnMapping.mode" = "name"
)
AS
SELECT
  current_timestamp()              AS load_dt,
  _metadata.file_path              AS source_file_path,
  _metadata.file_name              AS source_file_name,
  to_date(
    regexp_extract(_metadata.file_name, '(\\d{4}-\\d{2}-\\d{2})', 1)
  )                                AS source_file_date,
  *
FROM STREAM cloud_files(
  "/Volumes/workspace/damg7370/datastore/checkpoint",
  "csv",
  map("header","true","delimiter","|")
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE mm_silver
TBLPROPERTIES ("delta.columnMapping.mode" = "name")
AS
WITH cleaned AS (
  SELECT
    material_id,
    material_name,
    category,
    sub_category,
    UPPER(COALESCE(uom, 'NOS'))                                             AS uom,
    TRY_CAST(regexp_replace(unit_cost, ',', '') AS DOUBLE)                  AS unit_cost,
    supplier_name,
    country,
    plant,
    status,
    TO_DATE(last_updated)                                                   AS last_updated,
    TRY_CAST(lead_time_days AS INT)                                         AS lead_time_days,
    TRY_CAST(safety_stock   AS INT)                                         AS safety_stock,
    TRY_CAST(reorder_level  AS INT)                                         AS reorder_level,
    COALESCE(remarks, '')                                                   AS remarks,
    load_dt,
    source_file_path,
    source_file_name,
    source_file_date
  FROM STREAM mm_bronze
)
-- single "expectation": keep only rows with a valid, nonnegative unit_cost
SELECT *
FROM cleaned
WHERE unit_cost IS NOT NULL AND unit_cost >= 0;