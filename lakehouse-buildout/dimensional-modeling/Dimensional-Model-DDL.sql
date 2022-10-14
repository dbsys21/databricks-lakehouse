-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Unity Catalog : Support for Identity Columns, Primary + Foreign Key Constraints
-- MAGIC 
-- MAGIC <img src="https://github.com/dbsys21/databricks_lakehouse/raw/main/lakehouse-buildout/dimensional_modeling/resources/dimensiona-model-design-crop.png" style="float:right; margin:10px 0px 0px 10px" width="700"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC To simplify SQL operations and support migrations from on-prem and alternative warehouse, Databricks Lakehouse now give customers convenient ways to build Entity Relationship Diagrams that are simple to maintain and evolve.
-- MAGIC 
-- MAGIC These features offer:
-- MAGIC - The ability to automatically generate auto-incrementing identify columns. Just insert data and the engine will automatically increment the ID.
-- MAGIC - Support for defining primary key
-- MAGIC - Support for defining foreign key constraints
-- MAGIC 
-- MAGIC Note that as of now, Primary Key and Foreign Key are informational only and then won’t be enforced. 
-- MAGIC 
-- MAGIC <br /><br /><br />
-- MAGIC ## Use case
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/dbsys21/databricks_lakehouse/main/lakehouse-buildout/dimensional_modeling/resources/star-schema.png" style="float:right; margin:10px 0px 0px 10px" width="700"/>
-- MAGIC 
-- MAGIC Defining PK & FK helps the BI analyst to understand the entity relationships and how to join tables. It also offers more information to BI tools who can leverage this to perform further optimisation.
-- MAGIC 
-- MAGIC We'll define the following star schema:
-- MAGIC * dim_date
-- MAGIC * dim_store
-- MAGIC * dim_product
-- MAGIC * dim_customer
-- MAGIC 
-- MAGIC And the fact table containing our sales information pointing to our dimention tables:
-- MAGIC 
-- MAGIC * fact_sales
-- MAGIC 
-- MAGIC Requirements:
-- MAGIC - PK/FK requires Unity Catalog enabled (Hive Metastore is not supported for FK/PK)
-- MAGIC - DBR 11.1
-- MAGIC 
-- MAGIC <!-- tracking, please do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Fpk_fk%2Facl&dt=FEATURE_UC_PK_KF">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- DBTITLE 1,Environment Setup
CREATE CATALOG IF NOT EXISTS US_Stores;
USE CATALOG US_Stores;
CREATE SCHEMA IF NOT EXISTS Sales_DW;
USE SCHEMA Sales_DW;

-- COMMAND ----------

-- MAGIC %md ## 1/ Create a Dimension & Fact Tables In Unity Catalog
-- MAGIC 
-- MAGIC The first step is to create a Delta Tables in Unity Catalog.
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support (we could have done it in python too):
-- MAGIC 
-- MAGIC 
-- MAGIC * Use the `CREATE TABLE` command
-- MAGIC * Add generated identity column with `GENERATED ALWAYS AS IDENTITY` 
-- MAGIC * Define PK with `PRIMARY KEY`
-- MAGIC * Define Foreign Keys with `FOREIGN KEY REFERENCES`

-- COMMAND ----------

-- DBTITLE 1,Create Dimension Tables
-- Store dimension
CREATE OR REPLACE TABLE dim_store(
  store_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  business_key STRING,
  name STRING,
  email STRING,
  city STRING,
  address STRING,
  phone_number STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
);

-- Product dimension
CREATE OR REPLACE TABLE dim_product(
  product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  type STRING,
  SKU STRING,
  name STRING,
  description STRING,
  sale_price DOUBLE,
  regular_price DOUBLE,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
);

-- Customer dimension
CREATE OR REPLACE TABLE dim_customer(
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  name STRING,
  email STRING,
  address STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
);

-- Date dimension
CREATE OR REPLACE TABLE dim_date(
  date_id BIGINT PRIMARY KEY,
  date_num INT,
  date STRING,
  year_month_number INT,
  calendar_quarter STRING,
  month_num INT,
  month_name STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  start_at TIMESTAMP,
  end_at TIMESTAMP
);

-- COMMAND ----------

-- DBTITLE 1,Describe Dim Store table information
DESC TABLE EXTENDED US_Stores.Sales_DW.dim_store

-- COMMAND ----------

-- DBTITLE 1,Create Fact Table
-- Fact Sales
CREATE OR REPLACE TABLE fact_sales(  
  transaction_id BIGINT PRIMARY KEY,
  date_id BIGINT NOT NULL CONSTRAINT dim_date_fk FOREIGN KEY REFERENCES dim_date,
  customer_id BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
  product_id BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
  store_id BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
  store_business_key STRING,
  sales_amount DOUBLE
);

-- COMMAND ----------

-- DBTITLE 1,Describe Fact Sales Table Info
DESC TABLE EXTENDED US_Stores.Sales_DW.fact_sales

-- COMMAND ----------

-- DBTITLE 1,Add additional constraints  
-- Add constraint to dim_store to make sure column store_id is between 1 and 9998
ALTER TABLE US_Stores.Sales_DW.dim_store ADD CONSTRAINT valid_store_id CHECK (store_id > 0 and store_id < 9999);

-- Add constraint to fact_sales to make sure column sales_amount has a valid value
ALTER TABLE US_Stores.Sales_DW.fact_sales ADD CONSTRAINT valid_sales_amount CHECK (sales_amount > 0);

-- COMMAND ----------

-- MAGIC %md ## 2/ Let's look at the table definition for DIM_CUSTOMER
-- MAGIC 
-- MAGIC The first step is to run DESCRIBE TABLE EXTENDED
-- MAGIC 
-- MAGIC Constraints are shown at the bottom of the results:
-- MAGIC 
-- MAGIC 
-- MAGIC | col_name       | data_type                | 
-- MAGIC |----------------|--------------------------|
-- MAGIC | #  Constraints |                          |
-- MAGIC | dim_customer_pk    | PRIMARY KEY (`customer_id`) |

-- COMMAND ----------

DESCRIBE TABLE EXTENDED dim_customer;

-- COMMAND ----------

-- MAGIC %md ## 3/ Let's add some data to the Dimension Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 
-- MAGIC * Use the `INSERT INTO` command to insert some rows in the table
-- MAGIC * Note that we don't specify the values for IDs as they'll be generated by the engine with auto-increment

-- COMMAND ----------

-- Insert sample data for dimension tables
INSERT INTO
  dim_store (business_key, name, email, city, address, phone_number, created_date, updated_date, start_at, end_at)
VALUES
  ("PER01", "Perth CBD", "yhernandez@example.com", "Perth", "Level 2 95 Jorge Vale St. Gary, NT, 2705", "08-9854-6006", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("BNE02", "Brisbane Airport" , "castillojoseph@example.net", "Brisbane", "6 Ware Copse Doughertystad, NSW, 2687", "0425.061.371", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  dim_product (type, SKU, name, description, sale_price, regular_price, created_date, updated_date, start_at, end_at)
VALUES 
  ("variable", "vneck-tee", "V-Neck T-Shirt", "This is a variable product of type vneck-tee", "60.00", "50.00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("simple", "hoodie", "Hoodie", "This is a simple product of type hoodie", "90.00", "79.00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  dim_customer (name, email, address, created_date, updated_date, start_at, end_at)
VALUES 
  ("Stephanie Brown", "howardalejandra@example.net", "8273 Jerry Pine East Angela, ID 50196", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("Christopher Cooper", "campbelljohn@example.net", "8273 Jerry Pine East Angela, ID 50196", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  ("Daniel White", "colonricardo@example.net", "945 Goodwin Plain Suite 312 Dylanmouth, NY 14319", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);
  
INSERT INTO
  dim_date (date_id, date_num, date, year_month_number, calendar_quarter, month_num, month_name, created_date, updated_date, start_at, end_at)
VALUES 
  (20211001, 20211001, "2021-10-01", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  (20211002, 20211002, "2021-10-02", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL),
  (20211003, 20211003, "2021-10-03", 202110, "Qtr 4", 10, "October", "2021-10-01 00:00:00", "2021-10-01 00:00:00", "2021-10-01 00:00:00", NULL);

-- COMMAND ----------

-- DBTITLE 1,As you can see the ids (GENERATED ALWAYS) are automatically generated with increment:
SELECT * FROM dim_customer;

-- COMMAND ----------

-- MAGIC %md ## 4/ Let's add some data to the Fact Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

-- COMMAND ----------

-- Inser sample data for sales fact table 
INSERT INTO
  fact_sales (transaction_id, date_id, customer_id, product_id, store_id, store_business_key, sales_amount)
VALUES
  (10001, 20211001, 1, 1, 1, "PER01", 50.00),
  (10002, 20211002, 2, 1, 2, "BNE02", 79.00),
  (10003, 20211002, 1, 2, 2, "BNE02", 79.00),
  (10004, 20211003, 2, 1, 2, "BNE02", 60.00),
  (10005, 20211003, 3, 2, 1, "PER01", 79.00);

-- COMMAND ----------

SELECT * 
FROM US_Stores.Sales_DW.fact_sales

-- COMMAND ----------

-- DBTITLE 1,Optimize table using ZORDER
-- Optimise fact_sales table by customer_id and product_id for better query and join performance
OPTIMIZE US_Stores.Sales_DW.fact_sales 
ZORDER BY (customer_id, product_id); 

-- COMMAND ----------

-- DBTITLE 1,Create Bloomfilter index
-- Create a bloomfilter index to enable data skipping on store_business_key 
CREATE BLOOMFILTER INDEX
ON TABLE US_Stores.Sales_DW.fact_sales 
FOR COLUMNS(store_business_key)

-- COMMAND ----------

-- DBTITLE 1,Compute Table Statistics
-- collect stats for all columns for better performance
ANALYZE TABLE US_Stores.Sales_DW.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;

-- COMMAND ----------

-- MAGIC %md ### Query the tables joining data
-- MAGIC 
-- MAGIC We can now imply query the tables to retrieve our data based on the FK:

-- COMMAND ----------

SELECT * FROM fact_sales
  INNER JOIN dim_date     USING (date_id)
  INNER JOIN dim_product  USING (product_id)
  INNER JOIN dim_customer USING (customer_id)
  INNER JOIN dim_store    USING (store_id)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 5/ Primary Key and Foreign Key in Data Explorer
-- MAGIC 
-- MAGIC <br />
-- MAGIC 
-- MAGIC <img src="https://github.com/althrussell/databricks-demo/raw/main/product-demos/pkfk/images/data_explorer.gif" style="float:right; margin-left:100px" width="700"/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## 6/ Primary Key and Foreign Key in DBSQL - Code Completion
-- MAGIC 
-- MAGIC <br />
-- MAGIC 
-- MAGIC <img src="https://github.com/althrussell/databricks-demo/raw/main/product-demos/pkfk/images/code_completion.gif" style="float:center; margin-left:100px" width="700"/>

-- COMMAND ----------

-- DBTITLE 1,Updating sales amount from 79 to 89. 
UPDATE fact_sales 
SET sales_amount = '89.00' 
WHERE product_id = 2

-- COMMAND ----------

SELECT * FROM fact_sales

-- COMMAND ----------

-- DBTITLE 1,DELETE works even if underlying data is on external files 
DELETE FROM fact_sales 
WHERE transaction_id = 10001

-- COMMAND ----------

-- DBTITLE 1,Delta - Time Travel- ACID compliant Delta Log
DESCRIBE HISTORY fact_sales

-- COMMAND ----------

-- DBTITLE 1,Current version of tables has sales amount of 89 for product ID = 2
SELECT * FROM fact_sales
WHERE product_id = 2

-- COMMAND ----------

-- DBTITLE 1,Older version of tables still has sales amount of 79 for product ID = 2
SELECT * FROM fact_sales VERSION as of 5
WHERE product_id = 2

-- COMMAND ----------

-- DBTITLE 1,MERGE Operation for ETL ( used in SCD Type 1, SCD Type 2) 
MERGE INTO fact_sales F USING (
  SELECT
    *
  FROM
    FACT_SALES VERSION AS OF 5
) AS O ON F.product_id = O.product_id
AND F.transaction_id = O.transaction_id
WHEN MATCHED THEN
UPDATE
SET
  F.sales_amount = O.sales_amount --WHEN NOT MATCHED THEN..

-- COMMAND ----------

-- DBTITLE 1,Updated sales amount of 89 to 79 for product_id = 2
SELECT * FROM fact_sales
WHERE product_id = 2

-- COMMAND ----------

-- DBTITLE 1,Clean Up
DROP CATALOG IF EXISTS US_Stores CASCADE

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Summary
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/dbsys21/databricks_lakehouse/main/lakehouse-buildout/dimensional_modeling/resources/star-schema.png" style="float:right; margin-left:10px" width="900"/>
-- MAGIC 
-- MAGIC As you have seen Primary Keys and Foreign Keys help the BI analyst to understand the entity relationships and how to join tables and even better having code completion do the joins for you.  
-- MAGIC 
-- MAGIC The best Datawarehouse is a Lakehouse!
-- MAGIC 
-- MAGIC Next Steps:
-- MAGIC - Try DBSQL query & dashboard editors
-- MAGIC - Plug your BI tools (Tableau, PowerBI ...) to query these tables directly!
