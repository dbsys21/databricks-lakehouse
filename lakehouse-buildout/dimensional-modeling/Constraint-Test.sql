-- Databricks notebook source
CREATE TABLE body_health (id INT, name STRING, heart_rate INT);

-- COMMAND ----------

-- Insert some initial data to the table 
INSERT INTO
  body_health (id, name, heart_rate)
VALUES
  (10001, "David", 50),
  (10002, "Tim", 79),
  (10003, "Tom", 90),
  (10004, "Mary", 60),
  (10005, "Rose", 120);

-- COMMAND ----------


-- Add constraint to body_health to the heart_rate is between 60 to 100
ALTER TABLE body_health ADD CONSTRAINT heart_rate_speed CHECK (heart_rate >= 60 AND heart_rate <= 100);

-- COMMAND ----------

-- fix the data by setting all values to the lower bound if they are smaller than the minimum
UPDATE body_health
SET heart_rate = 60
WHERE heart_rate < 60

-- COMMAND ----------

-- fix the data by setting all values to the upper bound if they are bigger than the maximum
UPDATE body_health
SET heart_rate = 100
WHERE heart_rate > 100

-- COMMAND ----------


-- Add the contraint again after fixing the existing data, this should run successfully
ALTER TABLE body_health ADD CONSTRAINT heart_rate_speed CHECK (heart_rate >= 60 AND heart_rate <= 100);

-- COMMAND ----------

-- test to insert data that are out of constraint range, one within the range, one not 
-- it should fail as we have constraint enforce data quality
INSERT INTO
  body_health (id, name, heart_rate)
VALUES
  (10006, "Ivy", 80),   
  (10007, "Leo", 130);

-- COMMAND ----------

-- check the results again, no partial rows are inserted
SELECT *
FROM body_health

-- COMMAND ----------

DROP TABLE body_health;

-- COMMAND ----------


