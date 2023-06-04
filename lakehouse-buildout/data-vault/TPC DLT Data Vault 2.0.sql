-- Databricks notebook source
-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

CREATE OR REFRESH STREAMING LIVE TABLE raw_customer_A 
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

-- simulating another source to demonstrate if the data are coming from a different source
CREATE OR REFRESH STREAMING LIVE TABLE raw_customer_B 
COMMENT "RAW Customer Data from source B"
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

CREATE  STREAMING LIVE VIEW raw_customer_vw
COMMENT "RAW Customer Data View"
AS  SELECT
        sha1(UPPER(TRIM(c_custkey))) as sha1_hub_custkey,
        sha1(concat(UPPER(TRIM(c_name)),UPPER(TRIM(c_address)),UPPER(TRIM(c_phone)),UPPER(TRIM(c_mktsegment)))) as hash_diff,
        current_timestamp as load_ts,
        "Customer Source A" as source,
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment
    FROM STREAM(LIVE.raw_customer_A)
    UNION
    SELECT
        -- generate a random number to simulate a different business key
        sha1(concat(UPPER(TRIM(c_custkey)), cast(floor(rand()*101) as STRING))) as sha1_hub_custkey,
        sha1(concat(UPPER(TRIM(c_name)),UPPER(TRIM(c_address)),UPPER(TRIM(c_phone)),UPPER(TRIM(c_mktsegment)))) as hash_diff,
        current_timestamp as load_ts,
        "Customer Source B" as source,
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment
    FROM STREAM(LIVE.raw_customer_B)

-- COMMAND ----------

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

-- Use this property until the DLT Enzyme Bug gets resolved
SET pipelines.metrics.reportOnEmptyExpectations.disabled=false;

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

CREATE OR REFRESH STREAMING LIVE TABLE sat_customer(
  sha1_hub_custkey        STRING    NOT NULL,
  c_name                    STRING,
  c_address                 STRING,
  c_nationkey               BIGINT,
  c_phone                   STRING,
  c_acctbal                 DECIMAL(18,2),
  c_mktsegment              STRING,
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

CREATE OR REFRESH STREAMING LIVE TABLE hub_customer(
  sha1_hub_custkey        STRING     NOT NULL,
  c_custkey                 BIGINT     NOT NULL,
  load_ts                 TIMESTAMP,
  source                  STRING
  CONSTRAINT valid_sha1_hub_custkey EXPECT (sha1_hub_custkey IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_custkey EXPECT (c_custkey IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT " HUb CUSTOMER TABLE"
AS SELECT
      sha1_hub_custkey ,
      c_custkey,
      load_ts,
      source
   FROM
      STREAM(live.raw_customer_vw)
      

-- COMMAND ----------

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

CREATE  STREAMING LIVE VIEW raw_orders_vw 
COMMENT "RAW Order Data View"
AS  SELECT
        sha1(UPPER(TRIM(o_orderkey))) as sha1_hub_orderkey,
        sha1(concat(UPPER(TRIM(o_orderkey)),UPPER(TRIM(o_custkey)))) as sha1_lnk_customer_order_key,
        sha1(UPPER(TRIM(o_custkey)))  as sha1_hub_custkey,
        sha1(concat(UPPER(TRIM(o_orderstatus)),UPPER(TRIM(o_totalprice)),UPPER(TRIM(o_orderpriority)),UPPER(TRIM(o_shippriority)))) as hash_diff,
        current_timestamp as load_ts,
        "Order" as source,
        * 
    FROM STREAM(LIVE.raw_orders)

-- COMMAND ----------

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

CREATE  STREAMING LIVE VIEW raw_lineitem_vw 
COMMENT "RAW LineItem View"
AS  SELECT
        sha1(concat(UPPER(TRIM(l_orderkey)),UPPER(TRIM(l_linenumber)))) as sha1_hub_lineitem,
        sha1(UPPER(TRIM(l_orderkey))) as sha1_hub_orderkey,
        current_timestamp as load_ts,
        "LineItem" as source,
        * 
    FROM STREAM(LIVE.raw_lineitem)

-- COMMAND ----------

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
        current_timestamp as load_ts,
        "Region" as source
   FROM
        live.raw_region

-- COMMAND ----------

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
        current_timestamp as load_ts,
        "Nation" as source
   FROM
        live.raw_nation

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE sat_order_bv
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

CREATE LIVE VIEW sat_customer_bv
	   AS
	   SELECT 
             src.sha1_hub_custkey      AS sha1_hub_custkey,
	         src.source                AS source,                  
	         src.c_name                AS c_name ,             
	         src.c_address             AS c_address ,             
	         src.c_phone               AS c_phone ,              
	         src.c_acctbal             AS c_acctbal,             
	         src.c_mktsegment          AS c_mktsegment,                          
	         src.c_nationkey           AS c_nationkey,          
	        -- derived 
	         nation.n_name             AS nation_name,
	         region.r_name             AS region_name
	     FROM LIVE.sat_customer          src
	     LEFT OUTER JOIN 
              LIVE.ref_nation nation
	             ON (src.c_nationkey = nation.n_nationkey)
	     LEFT OUTER JOIN 
              LIVE.ref_region region
	             ON (nation.n_regionkey = region.r_regionkey)
           
	   ;
