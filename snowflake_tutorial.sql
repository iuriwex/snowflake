-- Set your role to SYSADMIN. To do so, run the following SQL.
USE ROLE SYSADMIN;

-- Create a new virtual warehouse with the name COMPUTE_WH.
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE = 'SMALL' 
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE;

-- Grant the virtual warehouse to the PUBLIC role so that any user in your account can use the virtual warehouse. To do so, run the following SQL.
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO PUBLIC;

-- Check that the virtual warehouse is setup correctly by running a test query e.g.
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER;

USE MY_FIRST_DATABASE
CREATE DATABASE MY_FIRST_DATABASE
CREATE TABLE OUR_FIRST_TABLE (
    first_name STRING,
    last_name STRING,
    address STRING,
    city STRING,
    state STRING
);

SELECT * FROM OUR_FIRST_TABLE;

-- MAke Snowflake aware of S3 bucket
--https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/our_first_table_data.csv
CREATE OR REPLACE stage my_s3_stage url='s3://snowflake-esentials/'


COPY INTO OUR_FIRST_TABLE
    FROM s3://snowflake-essentials/our_first_table_data.csv
    file_format = (type = csv field_delimiter = '|' skip_header = 1);



SELECT COUNT(*) FROM OUR_FIRST_TABLE;



-- 1. Make sure that you have the SYSADMIN role or higher.
USE ROLE SYSADMIN;


-- 2. To create a basic virtual warehouse, use the following syntax.
CREATE WAREHOUSE DBA_WH
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 300 -- automatically suspend the virtual warehouse after 5 minutes of inactivity
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE -- create the virtual warehouse in a suspended state
COMMENT = 'X-Small virtual warehouse for database administrator queries';


-- 3. Drop the virtual warehouse, so that we can recreate it through the WebUI.
DROP WAREHOUSE DBA_WH;


-- 4. Now you will re-create the virtual warehouse through the WebUI. Make sure that your role is set to SYSADMIN or higher. Click on the “Warehouse” button.


-- the SQL method for a variety of reasons.

-- The Snowflake WebUI doesn't show settings like "INITIALLY_SUSPENDED", "RESOURCE_MONITOR" and other parameters such as "MAX_CONCURRENCY_LEVEL", but you can configure these settings through the SQL syntax.

-- Using SQL removes the dependency on the user interface, and eases any future automation.





---
--- INGESTION
---


CREATE DATABASE ingest_data

CREATE TABLE customer (
    Customer_ID string,
    Customer_Name string,
    Customer_Email string,
    Customer_City string,
    Customer_State string,
    Customer_DOB DATE
);



-- Creating a staging area. Create an external stage using the S3 bucket that we create in the last step
CREATE OR REPLACE stage bulk_copy_example_stage url='s3://snowflake-essentials/ingesting_data/new_customer'

-- https://snowflake-essentials.s3-ap-southeast-2.amazonaws.com/ingesting_data/new_customer/2019-09-24/generated_customer_data.csv

-- list the files in the bucket
list @bulk_copy_example_stage;

use database ingest_data;

COPY INTO customer
    FROM @bulk_copy_example_stage
    pattern='.*.csv'
    file_format=(type = csv field_delimiter = '|' skip_header = 1)

SELECT COUNT(*) FROM customer


COPY INTO customer
    FROM @bulk_copy_example_stage/2019-09-24/additional_data.txt
    pattern='.*.csv'
    file_format=(type = csv field_delimiter = '|' skip_header = 1)

SELECT COUNT(*) FROM customer


COPY INTO customer
    FROM @bulk_copy_example_stage/2019-09-24/generated_customer_data.txt
    pattern='.*.csv'
    file_format=(type = csv field_delimiter = '|' skip_header = 1);
    
SELECT COUNT(*) FROM customer






USE DATABASE ingest_data


CREATE TABLE organisations_json_raw (
    json_data_raw VARIANT
)


-- create an external stage using the s3 buket that contains the JSON data
CREATE OR REPLACE STAGE json_example_stage url='s3://snowflake-essentials/json_data'

-- list the files in the bucket
LIST @json_example_stage

-- copy the example_json_file.json into the raw table
COPY INTO organisations_json_raw
    FROM @json_example_stage/example_json_file.json
    file_format = (type = json)


SELECT * FROM organisations_json_raw

SELECT
    json_data_raw:data_set,
    json_data_raw:extract_date
FROM organisations_json_raw


-- use flatten table to convert the json column
SELECT 
    value:name::String,
    value:state::String,
    value:org_code::String,
    json_data_raw:extract_date
FROM
    organisations_json_raw
    ,lateral flatten(input => json_data_raw:organisations );




-- JSON ASsignement
-- Stage JSON file into Cloud storage
-- Use snowflake JSON capabilities to write SQL which parses & convert the JSON into relational format
-- Use th SQL n step 2) in COPY command to load the results in to Snowflake


-- create raw table to store json data
CREATE TABLE customers_json_raw (
    json_data_raw VARIANT
)

-- create an external stage using the s3 buket that contains the JSON data
CREATE OR REPLACE STAGE json_customers_stage url='s3://snowflake-essentials-json-lab'

-- list files available in the stage
LIST @json_customers_stage 

-- copy data into the stagging table 
COPY INTO customers_json_raw
    FROM @json_customers_stage/sample_data.json
    file_format = (type = json);


SELECT * FROM customers_json_raw;

SELECT 
    json_data_raw:cdc_date
FROM customers_json_raw;


SELECT
    value:Customer_ID::String,
    json_data_raw:cdc_date
FROM
    customers_json_raw
    ,lateral flatten(input => json_data_raw:customers );


SELECT
    COUNT(value:Customer_ID::String),
    json_data_raw:cdc_date AS cdc_date
FROM
    customers_json_raw
    ,lateral flatten(input => json_data_raw:customers )
GROUP BY cdc_date;


SELECT
    COUNT(value:Customer_ID::String),
    value:Customer_City::String AS city,
    json_data_raw:cdc_date AS cdc_date
FROM
    customers_json_raw
    ,lateral flatten(input => json_data_raw:customers )
WHERE city = 'Cornwall'
GROUP BY city, cdc_date;




-- Snowpipe
-- Snowpipe is a mechanism to enable loading of data as soon as it becomes available in a stage
-- Using snowpipe you can achieve micro-batched loadig of data
-- It is usually used where there is a continuously arriving of data such as transations or events and there is need to make the data available to bussiness immediately 
-- Snowpipe uses server less architecture, so it will not use an virtual warehouse instance but has its own processing nad is billed diffferently
-- Snowpipe definitions contain a COPY statement which is used by Snowflake to load the data
-- The snowppipe may be automattically or mannually triggered to load data

-- CREATE an external stage using a S3 bucket
CREATE OR REPLACE STAGE snowpipe_copy_example_stage url='s3://snowflake-essentials-streaming/transactions';

-- list the files in the bucket
LIST @snowpipe_copy_example_stage;


CREATE TABLE transactions (
    Transaction_Date DATE,
    Customer_ID NUMBER,
    Transaction_ID NUMBER,
    Amount NUMBER
);

COPY INTO transactions FROM @snowpipe_copy_example_stage
file_format = (type = csv field_delimiter = '|' skip_header= 1);


CREATE OR REPLACE PIPE transaction_pipe
auto_ingest = true
AS COPY INTO transactions FROM @snowpipe_copy_example_stage
file_format = (type= csv field_delimiter = '|' skip_header = 1);

SELECT * FROM transactions;

SHOW PIPES;



-- For better perfomance optmization, use micro partitions, with a balanced sizing (nor too small neither too large) 
-- Use dedicated virutal warehoues
-- scale up for known large workloads
-- scale out for unknown & unexpected workloads
-- design to maximize cache usage 

-- grant USAGE t oa database to public role
GRANT USAGE ON DATABASE ingest_data TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA ingest_data.PUBLIC TO ROLE PUBLIC;
GRANT SELECT ON TABLE ingest_data.PUBLIC.TRANSACTIONS TO ROLE PUBLIC;

-- virtual warehouse for the data scientist
CREATE WAREHOUSE DATASCIENCE_WH WITH WAREHOUSE_SIZE= 'SMALL' WAREHOUSE_TYPE= 'STANDARD' AUTO_SUSPEND= 300 AUTO_RESUME= TRUE;

-- virtual warehouse for the DBAs. It is sized extra small since DBA query should normally be of short duration
CREATE WAREHOUSE DBA_WH WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME=TRUE;

CREATE ROLE DATA_SCIENTISTS;
GRANT USAGE ON WAREHOUSE DATASCIENCE_WH TO ROLE DATA_SCIENTISTS;

CREATE ROLE DBAS;
GRANT USAGE ON WAREHOUSE DBA_WH TO ROLE DBAS 


CREATE USER DS_1 PASSWORD = 'DS_1' LOGIN_NAME = 'DS_1' DEFAULT_ROLE = 'DATA_SCIENTIS' DEFAULT_WAREHOUSE = 'DATASCIENCE_WH' MUST_CHANGE_PASSWORD = FALSE;
CREATE USER DS_2 PASSWORD = 'DS_2' LOGIN_NAME = 'DS_2' DEFAULT_ROLE = 'DATA_SCIENTIS' DEFAULT_WAREHOUSE = 'DATASCIENCE_WH' MUST_CHANGE_PASSWORD = FALSE;



CREATE USER DBA_1 PASSWORD = 'DBA_1' LOGIN_NAME = 'DBA_1' DEFAULT_ROLE = 'DBAS' DEFAULT_WAREHOUSE = 'DBA_WH' MUST_CHANGE_PASSWORD = FALSE;
CREATE USER DBA_2 PASSWORD = 'DBA_2' LOGIN_NAME = 'DBA_2' DEFAULT_ROLE = 'DBAS' DEFAULT_WAREHOUSE = 'DBA_WH' MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DBAS TO USER DBA_1;
GRANT ROLE DBAS TO USER DBA_2;


SELECT COUNT(*) FROM transactions WHERE transaction_date = '2019-09-02'



