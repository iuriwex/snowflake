-- create a database if it doesn't already exist
CREATE DATABASE ingest_data;

USE DATABASE ingest_data;


-- create an external stage using an S3 bucket
CREATE OR REPLACE STAGE snowpipe_copy_example_stage url='s3://snowpipe-streaming/transactions';

-- list the files in the bucket
LIST @snowpipe_copy_example_stage;

CREATE TABLE transactions
(

Transaction_Date DATE,
Customer_ID NUMBER,
Transaction_ID NUMBER,
Amount NUMBER
);

CREATE OR REPLACE PIPE transaction_pipe 
auto_ingest = true
AS COPY INTO transactions FROM @snowpipe_copy_example_stage
file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM transactions;

SHOW PIPES;

-- setup S3 event notification here

SELECT COUNT(*) FROM transactions;
