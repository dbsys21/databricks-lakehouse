-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 
-- MAGIC ### 1. Create raw tables

-- COMMAND ----------

-- create raw customer table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_customer 
COMMENT "RAW Customer Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/customer", "parquet",
      map("schema", 
          " 
          c_custkey     bigint,
          c_name        string,
          c_address     string,
          c_nationkey   bigint,
          c_phone       string,
          c_acctbal     decimal(18,2),
          c_mktsegment  string,
          c_comment     string
          "
           )
      )

-- COMMAND ----------

-- create raw customer view and add hash colomns for checking existence or comparison 
CREATE STREAMING LIVE VIEW raw_customer_vw
COMMENT "RAW Customer Data View"
AS  SELECT
        sha1(UPPER(TRIM(c_custkey))) as sha1_hub_custkey,
        sha1(concat(UPPER(TRIM(c_name)),UPPER(TRIM(c_address)),UPPER(TRIM(c_phone)),UPPER(TRIM(c_mktsegment)))) as hash_diff,
        current_timestamp as load_ts,
        "Customer Source" as source,
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment
    FROM STREAM(LIVE.raw_customer)

-- COMMAND ----------

-- create raw order table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_orders 
COMMENT "RAW Orders Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/orders", "parquet",
      map("schema", 
          " 
          o_orderkey       bigint,
          o_custkey        bigint,
          o_orderstatus    string,
          o_totalprice     decimal(18,2),
          o_orderdate      date,
          o_orderpriority  string,
          o_clerk          string,
          o_shippriority   int,
          o_comment        string
          "
           )
      )

-- COMMAND ----------

-- create raw order view and add hash colomns for checking existence or comparison 
CREATE STREAMING LIVE VIEW raw_orders_vw 
COMMENT "RAW Order Data View"
AS  SELECT
        sha1(UPPER(TRIM(o_orderkey))) as sha1_hub_orderkey,
        sha1(concat(UPPER(TRIM(o_orderkey)),UPPER(TRIM(o_custkey)))) as sha1_lnk_customer_order_key,
        sha1(UPPER(TRIM(o_custkey)))  as sha1_hub_custkey,
        sha1(concat(UPPER(TRIM(o_orderstatus)),UPPER(TRIM(o_totalprice)),UPPER(TRIM(o_orderpriority)),UPPER(TRIM(o_shippriority)))) as hash_diff,
        current_timestamp as load_ts,
        "Order Source" as source,
        * 
    FROM STREAM(LIVE.raw_orders)

-- COMMAND ----------

-- create raw part table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_part 
COMMENT "RAW Parts Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/part", "parquet",
      map("schema", 
          " 
          p_partkey      bigint,
          p_name         string,
          p_mfgr         string,
          p_brand        string,
          p_type         string,
          p_size         int,
          p_container    string,
          p_retailprice  decimal(18,2),
          p_comment      string
          "
           )
      )

-- COMMAND ----------

-- create raw supplier table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_supplier
COMMENT "RAW Supplier Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/supplier", "parquet",
      map("schema", 
          " 
          s_suppkey      bigint,
          s_name         string,
          s_address      string,
          s_nationkey    bigint,
          s_phone        string,
          s_acctbal      decimal(18,2),
          s_comment      string
          "
           )
      )

-- COMMAND ----------

-- create region part table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_region 
COMMENT "RAW Region Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/region", "parquet",
      map("schema", 
          " 
          r_regionkey     bigint,
          r_name          string,
          r_comment       string
          "
           )
      )

-- COMMAND ----------

-- create raw nation table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_nation
COMMENT "RAW Nation Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/nation", "parquet",
      map("schema", 
          " 
          n_nationkey     bigint,
          n_name          string,
          n_regionkey     bigint,
          n_comment       string
          "
           )
      )

-- COMMAND ----------

-- create raw lineitem table from the landing files 
CREATE OR REFRESH STREAMING LIVE TABLE raw_lineitem 
COMMENT "RAW LineItem Data"
AS  SELECT 
      * 
    FROM 
      cloud_files("/databricks-datasets/tpch/delta-001/lineitem", "parquet",
      map("schema", 
          " 
          l_orderkey      bigint,
          l_partkey       bigint,
          l_suppkey       bigint,
          l_linenumber    int,
          l_quantity      decimal(18,2),
          l_extendedprice decimal(18,2),
          l_discount      decimal(18,2),
          l_tax           decimal(18,2),
          l_returnflag    string,
          l_linestatus    string,
          l_shipdate      date,
          l_commitdate    date,
          l_receiptdate   date,
          l_shipinstructs string,
          l_shipmode      string,
          l_comment       string
          "
           )
      )

-- COMMAND ----------

-- create raw lineitem view and add hash coloums for checking existence 
CREATE STREAMING LIVE VIEW raw_lineitem_vw 
COMMENT "RAW LineItem View"
AS  SELECT
        sha1(concat(UPPER(TRIM(l_orderkey)),UPPER(TRIM(l_linenumber)))) as sha1_hub_lineitem,
        sha1(UPPER(TRIM(l_orderkey))) as sha1_hub_orderkey,
        current_timestamp as load_ts,
        "LineItem Source" as source,
        * 
    FROM STREAM(LIVE.raw_lineitem)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### 2. Create Raw Vault

-- COMMAND ----------

-- create hub customer table from the raw customer view
CREATE OR REFRESH STREAMING LIVE TABLE hub_customer(
  sha1_hub_custkey        STRING     NOT NULL,
  c_custkey               BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
  CONSTRAINT valid_sha1_hub_custkey EXPECT (sha1_hub_custkey IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_custkey EXPECT (c_custkey IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1_hub_custkey,
      c_custkey,
      load_ts,
      source
   FROM
      STREAM(live.raw_customer_vw)

-- COMMAND ----------

-- create satellite customer table from raw customer view 
CREATE OR REFRESH STREAMING LIVE TABLE sat_customer(
  sha1_hub_custkey        STRING    NOT NULL,
  c_name                  STRING,
  c_address               STRING,
  c_nationkey             BIGINT,
  c_phone                 STRING,
  c_acctbal               DECIMAL(18,2),
  c_mktsegment            STRING,
  hash_diff               STRING    NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING    NOT NULL
  CONSTRAINT valid_sha1_hub_custkey EXPECT (sha1_hub_custkey IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT " SAT CUSTOMER TABLE"
AS SELECT
      sha1_hub_custkey,
      c_name,
      c_address,
      c_nationkey,
      c_phone,
      c_acctbal,
      c_mktsegment,
      hash_diff,
      load_ts,
      source
   FROM
      STREAM(live.raw_customer_vw)

-- COMMAND ----------

-- create hub orders table from the raw customer view
CREATE OR REFRESH STREAMING LIVE TABLE hub_orders(
  sha1_hub_orderkey       STRING     NOT NULL,
  o_orderkey              BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1_hub_orderkey ,
      o_orderkey,
      load_ts,
      source
   FROM
      STREAM(live.raw_orders_vw)

-- COMMAND ----------

-- create satellite orders table from the raw customer view
CREATE OR REFRESH STREAMING LIVE TABLE sat_orders(
  sha1_hub_orderkey         STRING    NOT NULL,
  o_orderstatus             STRING,
  o_totalprice              decimal(18,2),
  o_orderdate               DATE,
  o_orderpriority           STRING,
  o_clerk                   STRING,
  o_shippriority            INT,
  load_ts                   TIMESTAMP,
  source                    STRING    NOT NULL
)
COMMENT " SAT CUSTOMER TABLE"

AS SELECT
      sha1_hub_orderkey,
      o_orderstatus,
      o_totalprice,
      o_orderdate,
      o_orderpriority,
      o_clerk,
      o_shippriority,
      load_ts,
      source
   FROM
      STREAM(live.raw_orders_vw)

-- COMMAND ----------

-- create customer orders table from the raw orders view
CREATE OR REFRESH STREAMING LIVE TABLE lnk_customer_orders
(
  sha1_lnk_customer_order_key     STRING     NOT NULL ,  
  sha1_hub_orderkey               STRING ,
  sha1_hub_custkey                STRING ,
  load_ts                         TIMESTAMP  NOT NULL,
  source                          STRING     NOT NULL 
)
COMMENT " LNK CUSTOMER ORDERS TABLE "
AS SELECT
      sha1_lnk_customer_order_key,
      sha1_hub_orderkey,
      sha1_hub_custkey,
      load_ts,
      source
   FROM
       STREAM(live.raw_orders_vw)

-- COMMAND ----------

-- create hub lineitem table from the raw lineitem view
CREATE OR REFRESH STREAMING LIVE TABLE hub_lineitem(
  sha1_hub_lineitem        STRING     NOT NULL,
  sha1_hub_orderkey        STRING     NOT NULL,
  l_linenumber             int,
  load_ts                  TIMESTAMP,
  source                   STRING
)
COMMENT " HUb LINEITEM TABLE"
AS SELECT
      sha1_hub_lineitem,
      sha1_hub_orderkey,
      l_linenumber,
      load_ts,
      source
   FROM
      STREAM(live.raw_lineitem_vw)

-- COMMAND ----------

-- create satellite lineitem table from the raw lineitem view
CREATE OR REFRESH STREAMING LIVE TABLE sat_lineitem(
          sha1_hub_lineitem        STRING     NOT NULL,
          l_quantity               decimal(18,2),
          l_extendedprice          decimal(18,2),
          l_discount               decimal(18,2),
          l_tax                    decimal(18,2),
          l_returnflag             string,
          l_linestatus             string,
          l_shipdate               date,
          l_commitdate             date,
          l_receiptdate            date,
          l_shipinstructs          string,
          l_shipmode               string,
          load_ts                  TIMESTAMP,
          source                   STRING
)
COMMENT " SAT LINEITEM TABLE"
AS SELECT
          sha1_hub_lineitem,
          l_quantity,
          l_extendedprice,
          l_discount,
          l_tax,
          l_returnflag,
          l_linestatus,
          l_shipdate,
          l_commitdate,
          l_receiptdate,
          l_shipinstructs,
          l_shipmode,
          load_ts,
          source
   FROM
      STREAM(live.raw_lineitem_vw)

-- COMMAND ----------

-- create ref region table from the raw region table
CREATE OR REFRESH LIVE TABLE ref_region(
  r_regionkey        bigint     NOT NULL,
  r_name             STRING ,
  load_ts            TIMESTAMP,
  source             STRING
)
COMMENT " Ref Region Table"
AS SELECT
        r_regionkey,
        r_name,
        current_timestamp AS load_ts,
        "Region Source"   AS source
   FROM
        live.raw_region

-- COMMAND ----------

-- create ref nation table from the raw nation table
CREATE OR REFRESH LIVE TABLE ref_nation(
  n_nationkey        bigint     NOT NULL,
  n_name             STRING ,
  n_regionkey        bigint,
  load_ts            TIMESTAMP,
  source             STRING
)
COMMENT " Ref Nation Table"
AS SELECT
        n_nationkey,
        n_name,
        n_regionkey,
        current_timestamp AS load_ts,
        "Nation Source"   AS source
   FROM
        live.raw_nation

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### 3. Create Business Vault

-- COMMAND ----------

-- create satellite order table in business vault from the satellite orders table in raw vault 
CREATE OR REFRESH  LIVE TABLE sat_orders_bv
(
  sha1_hub_orderkey         STRING     NOT NULL ,  
  o_orderstatus             STRING ,
  o_totalprice              decimal(18,2) ,
  o_orderdate               DATE,
  o_orderpriority           STRING,
  o_clerk                   STRING,
  o_shippriority            INT,
  order_priority_tier       STRING,
  source                    STRING    NOT NULL
  
)
COMMENT " SAT Order Business Vault TABLE "
AS SELECT
          sha1_hub_orderkey     AS sha1_hub_orderkey,
          o_orderstatus         AS o_orderstatus,
		  o_totalprice          AS o_totalprice,
          o_orderdate           AS o_orderdate,
          o_orderpriority       AS o_orderpriority,
		  o_clerk               AS o_clerk,
          o_shippriority        AS o_shippriority,
		  CASE WHEN o_orderpriority IN ('2-HIGH', '1-URGENT') AND o_totalprice >= 225000 THEN 'Tier-1'
               WHEN o_orderpriority IN ('3-MEDIUM', '2-HIGH', '1-URGENT') AND o_totalprice BETWEEN 120000 AND 225000 THEN 'Tier-2'   
			   ELSE 'Tier-3'
		  END order_priority_tier,
          source
   FROM
       live.sat_orders

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### 4. Create data mart with views or tables

-- COMMAND ----------

-- create customer dimension as view in data mart from the hub and satellite customer table, ref nation and ref region table
CREATE LIVE VIEW dim_customer
       AS
       SELECT 
             sat.sha1_hub_custkey      AS dim_customer_key,
	         sat.source                AS source,                  
	         sat.c_name                AS c_name ,             
	         sat.c_address             AS c_address ,             
	         sat.c_phone               AS c_phone ,              
	         sat.c_acctbal             AS c_acctbal,             
	         sat.c_mktsegment          AS c_mktsegment,                          
	         sat.c_nationkey           AS c_nationkey,  
             sat.load_ts               AS c_effective_ts,
	         -- derived 
	         nation.n_name             AS nation_name,
	         region.r_name             AS region_name
	     FROM LIVE.hub_customer hub
         INNER JOIN LIVE.sat_customer sat
           ON hub.sha1_hub_custkey = sat.sha1_hub_custkey
	     LEFT OUTER JOIN LIVE.ref_nation nation
	       ON (sat.c_nationkey = nation.n_nationkey)
	     LEFT OUTER JOIN LIVE.ref_region region
	       ON (nation.n_regionkey = region.r_regionkey)

-- COMMAND ----------

-- create order dimension as view in data mart from the hub and satellite order table
CREATE LIVE VIEW dim_orders
       AS
       SELECT 
           hub.sha1_hub_orderkey                         AS dim_order_key,
           sat.*     
       FROM LIVE.hub_orders hub
       INNER JOIN LIVE.sat_orders_bv sat
       WHERE 
         hub.sha1_hub_orderkey = sat.sha1_hub_orderkey

-- COMMAND ----------

-- create fact customer order table in data mart from the lnk_customer_orders, dim_order, dim_customer, ref_nation and ref_region
CREATE OR REFRESH LIVE TABLE fact_customer_orders
       AS
       SELECT 
           dim_customer.dim_customer_key,
           dim_orders.dim_order_key,
           nation.n_nationkey     AS dim_nation_key,
           region.r_regionkey     AS dim_region_key,
           dim_orders.o_totalprice AS total_price,
           dim_orders.o_orderdate  AS order_date
       FROM LIVE.lnk_customer_orders lnk
       INNER JOIN LIVE.dim_orders dim_orders
           ON lnk.sha1_hub_orderkey = dim_orders.dim_order_key
       INNER JOIN LIVE.dim_customer dim_customer
           ON lnk.sha1_hub_custkey = dim_customer.dim_customer_key
	   LEFT OUTER JOIN LIVE.ref_nation nation
           ON dim_customer.c_nationkey = nation.n_nationkey
       LEFT OUTER JOIN LIVE.ref_region region
           ON nation.n_regionkey = region.r_regionkey
