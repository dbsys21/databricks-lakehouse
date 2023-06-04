-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables with SQL
-- MAGIC
-- MAGIC This notebook uses SQL to declare Delta Live Tables. 
-- MAGIC
-- MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic DLT SQL Syntax
-- MAGIC
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to tradtional CTAS statements.
-- MAGIC
-- MAGIC DLT tables and views will always be preceded by the `LIVE` keyword.
-- MAGIC
-- MAGIC If you wish to process data incrementally (using the same processing model as Structured Streaming), also use the `INCREMENTAL` keyword.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 1: Create Bronze table for Sales

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_date
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze Date table ingested from cloud object storage landing zone"
AS SELECT *, input_file_name() as input_file_name FROM cloud_files("/FileStore/lakehouse/demo/data/date", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_customer
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze Customer table incrementally ingested from cloud object storage landing zone"
AS SELECT *, input_file_name() as input_file_name FROM cloud_files("/FileStore/lakehouse/demo/data/customer", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_product
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze Product table incrementally ingested from cloud object storage landing zone"
AS SELECT *, input_file_name() as input_file_name FROM cloud_files("/FileStore/lakehouse/demo/data/product", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_store
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze Store table incrementally ingested from cloud object storage landing zone"
AS SELECT *, input_file_name() as input_file_name FROM cloud_files("/FileStore/lakehouse/demo/data/store", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_sale
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze sale table incrementally ingested from cloud object storage landing zone"
AS SELECT *, input_file_name() as input_file_name FROM cloud_files("/FileStore/lakehouse/demo/data/sale", "csv", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 2: Create a Silver table
-- MAGIC
-- MAGIC #### For fact table, e.g. Sales, it's straight inserts
-- MAGIC #### For dimension table, e.g. product, store, user, it's SCD type 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referencing Streaming Tables
-- MAGIC
-- MAGIC Queries against other DLT tables and views will always use the syntax `live.table_name`. At execution, the target database name will be substituted, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC
-- MAGIC When referring to another streaming DLT table within a pipeline, use the `STREAM(live.table_name)` syntax to ensure incremental processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quality Control with Constraint Clauses
-- MAGIC
-- MAGIC Data expectations are expressed as simple constraint clauses, which are essential where statements against a field in a table.
-- MAGIC
-- MAGIC Adding a constraint clause will always collect metrics on violations. If no `ON VIOLATION` clause is included, records violating the expectation will still be included.
-- MAGIC
-- MAGIC DLT currently supports two options for the `ON VIOLATION` clause.
-- MAGIC
-- MAGIC | mode | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `FAIL UPDATE` | Fail when expectation is not met |
-- MAGIC | `DROP ROW` | Only process records that fulfill expectations |
-- MAGIC
-- MAGIC
-- MAGIC Roadmap: `QUARANTINE`

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_date (
  CONSTRAINT date_id EXPECT (date_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csv_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, could use a view when required. We ensure valid csv, date_id for demonstration purpose"
AS SELECT * 
FROM STREAM(live.bronze_date);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_customer (
  CONSTRAINT customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csv_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, customer_id for demonstration purpose"
AS SELECT * 
FROM STREAM(live.bronze_customer);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_product (
  CONSTRAINT product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csv_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, product_id for demonstration purpose"
AS SELECT * 
FROM STREAM(live.bronze_product);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_store (
  CONSTRAINT valid_business_key EXPECT (business_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csv_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, we ensude valid csv, business_key for demonstration purpose"
AS SELECT * 
FROM STREAM(live.bronze_store);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_sale (
  CONSTRAINT transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT store EXPECT (store IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_csv_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed silver table, could use a view when required. We ensure valid csv, transaction_id, store for demonstration purpose"
AS SELECT * 
FROM STREAM(live.bronze_sale);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Step 3: Create Gold table

-- COMMAND ----------

-- create the gold table
CREATE STREAMING LIVE TABLE dim_date
TBLPROPERTIES ("quality" = "gold")
COMMENT "Static Date dimension in the gold layer"
AS SELECT * 
FROM STREAM(live.silver_date);

-- COMMAND ----------

-- create the gold table
CREATE INCREMENTAL LIVE TABLE dim_customer
TBLPROPERTIES ("quality" = "gold")
COMMENT "Slowly Changing Dimension Type 2 for customer dimension in the gold layer";

-- store all changes as SCD2
APPLY CHANGES INTO live.dim_customer
FROM stream(live.silver_customer)
  KEYS (customer_id)
  SEQUENCE BY updated_date
  COLUMNS * EXCEPT (_rescued_data, input_file_name)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- create the gold table
CREATE INCREMENTAL LIVE TABLE dim_product
TBLPROPERTIES ("quality" = "gold")
COMMENT "Slowly Changing Dimension Type 2 for product dimension in the gold layer";

-- store all changes as SCD2
APPLY CHANGES INTO live.dim_product
FROM stream(live.silver_product)
  KEYS (product_id)
  SEQUENCE BY updated_date
  COLUMNS * EXCEPT (_rescued_data, input_file_name)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- create the gold table
CREATE INCREMENTAL LIVE TABLE dim_store
TBLPROPERTIES ("quality" = "gold")
COMMENT "Slowly Changing Dimension Type 2 for store dimension in the gold layer";

-- store all changes as SCD2
APPLY CHANGES INTO live.dim_store
FROM stream(live.silver_store)
  KEYS (store_id)
  SEQUENCE BY updated_date
  COLUMNS * EXCEPT (_rescued_data, input_file_name)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- create the fact table for sales in gold layer
CREATE STREAMING LIVE TABLE fact_sale (
  CONSTRAINT valid_store_business_key EXPECT (store_business_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW
) 
TBLPROPERTIES ("quality" = "gold", "ignoreChanges" = "true")
COMMENT "sales fact table in the gold layer" AS
  SELECT
    sale.transaction_id,
    date.date_id,
    customer.customer_id,
    product.product_id AS product_id,
    store.store_id,
    store.business_key AS store_business_key,
    sales_amount
  FROM STREAM(live.silver_sale) sale
  INNER JOIN live.dim_date date
  ON to_date(sale.transaction_date, 'M/d/yy') = to_date(date.date, 'M/d/yyyy') 
  -- only join with the active customers
  INNER JOIN (SELECT * FROM live.dim_customer WHERE __END_AT IS NULL) customer
  ON sale.customer_id = customer.customer_id
  -- only join with the active products
  INNER JOIN (SELECT * FROM live.dim_product WHERE __END_AT IS NULL) product
  ON sale.product = product.SKU
  -- only join with the active stores
  INNER JOIN (SELECT * FROM live.dim_store WHERE __END_AT IS NULL) store
  ON sale.store = store.business_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Enrich dataset - create daily sales

-- COMMAND ----------

-- create the fact table for daily sales in gold layer
CREATE STREAMING LIVE TABLE fact_daily_sale (
  CONSTRAINT valid_date_id EXPECT (date_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sales_amount EXPECT (sales_amount IS NOT NULL) ON VIOLATION DROP ROW
) 
TBLPROPERTIES ("quality" = "gold")
COMMENT "daily sales fact table in the gold layer" AS
  SELECT
    sale.date_id,
    sale.customer_id,
    sale.product_id,
    sale.store_id,
    sale.store_business_key,
    sum(sales_amount) AS sales_amount
  FROM STREAM(live.fact_sale) sale
  GROUP BY 
    sale.date_id,
    sale.customer_id,
    sale.product_id,
    sale.store_id,
    sale.store_business_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Enrich dataset - create daily store sales

-- COMMAND ----------

-- create the fact table for daily store sales in gold layer
CREATE STREAMING LIVE TABLE fact_daily_store_sale (
  CONSTRAINT valid_date_id EXPECT (date_id IS NOT NULL) ON VIOLATION DROP ROW
) 
TBLPROPERTIES ("quality" = "gold")
COMMENT "daily sales fact table in the gold layer" AS
  SELECT
    sale.date_id,
    sale.store_id,
    sale.store_business_key,
    sum(sales_amount) AS sales_amount
  FROM STREAM(live.fact_sale) sale
  GROUP BY 
    sale.date_id,
    sale.store_id,
    sale.store_business_key
