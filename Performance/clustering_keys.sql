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

-- validate by running a query and checking for performance profile
SELECT COUNT(*) FROM transactions_large WHERE TRANSACTION_DATE = '2018-12-18';

-- create a new table clustered on date
-- we will demonstrate partition elimination through this table
CREATE TABLE transactions_clustered_date (
Transaction_Date DATE,
Transaction_ID integer ,
Customer_ID string,
Amount integer
) CLUSTER BY (Transaction_Date);

-- load this table with the same data which is contained in the transactions_large table
INSERT INTO transactions_clustered_date SELECT * FROM transactions_large;

-- validate partition elimination occurs when accessing this table via the transaction date
SELECT COUNT(*) FROM transactions_clustered_date WHERE TRANSACTION_DATE = '2018-12-18';

-- check how does the partition elimination occurs if we access a full month of data on a table paritioned on date
SELECT COUNT(*) FROM transactions_clustered_date WHERE TRANSACTION_DATE BETWEEN '2018-12-01' AND '2018-12-31';

-- create a new table which is partitioned on an expression
-- more specifically partitioned on month
CREATE TABLE transactions_clustered_month (
Transaction_Date DATE,
Transaction_ID integer ,
Customer_ID string,
Amount integer
) CLUSTER BY (date_trunc('MONTH', Transaction_Date));

-- load this table with the same data which is contained in the transactions_large table
INSERT INTO transactions_clustered_month SELECT * FROM transactions_large;

-- validate partition elimination occurs when accessing this table via the transaction date
SELECT COUNT(*) FROM transactions_clustered_month WHERE date_trunc('MONTH', Transaction_Date) = '2018-12-01';