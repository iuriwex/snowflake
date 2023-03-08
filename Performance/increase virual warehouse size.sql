-- create database where our performance testing table will reside
CREATE DATABASE performance_test;

USE DATABASE performance_test;

-- create transactions table, we will load this from S3 with data we previously 
-- used in streaming data example
CREATE TABLE transactions (
Transaction_Date DATE,
Transaction_ID integer ,
Customer_ID string,
Amount integer
);

-- create the stage pointing to this data
create or replace stage perf_stage url='s3://snowflake-essentials/streaming_data_ingest/Transactions';

-- list the files in the bucket
list @perf_stage;

-- load the data , 100 rows should load
copy into transactions
  from @perf_stage
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

-- create another transactions table, we will populate large 
-- amounts of data in this 
CREATE TABLE transactions_large (
Transaction_Date DATE,
Transaction_ID integer ,
Customer_ID string,
Amount integer
);

-- based on the just loaded transactions table we will cross join it to itself
-- several times over and produce 100 millions rows in the process
-- we will insert these 100M rows into the transactions_large table
-- transactions large table will then serve as our performance test base table
INSERT INTO transactions_large
SELECT a.Transaction_Date + MOD(RANDOM(), 2000), RANDOM(), a.Customer_ID,a.Amount 
FROM transactions a CROSS JOIN transactions b CROSS JOIN transactions c CROSS JOIN transactions d;



-- see the configuration for the data science virtual warehouse
SHOW WAREHOUSES LIKE 'DATASCIENCE_WH';

-- set the size to be LARGE
ALTER WAREHOUSE DATASCIENCE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- see the updated configuration for the data science virtual warehouse
SHOW WAREHOUSES LIKE 'DATASCIENCE_WH';
