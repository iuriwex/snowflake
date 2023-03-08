CREATE DATABASE customer_leads;

CREATE TABLE prospects (
  id STRING,
  name STRING ,
  address STRING ,
  city string ,
  postcode string ,
  state string,
  company string,
  contact string
  );


CREATE TABLE signedup_customers (
  id STRING,
  name STRING
  );

--sample data for the table above is present in AWS S3 at
--https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/customer.csv

create or replace stage my_s3_stage url='s3://snowflake-essentials/';

copy into prospects
  from s3://snowflake-essentials/customer.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM prospects;
SELECT COUNT(*) FROM prospects;




-- create the share object to which we will then add data to be shared. 
CREATE SHARE share_leads_databases;

-- grant usage on the database in which our table is contained
-- this step is necessary to subsequently provide access to the table
GRANT USAGE ON DATABASE customer_leads TO SHARE share_leads_databases;

-- grant usage on the schema in which our table is contained.
-- again this step is necessary to subsequently provide access to the table
GRANT USAGE ON SCHEMA customer_leads.public TO SHARE share_leads_databases;

-- grant select on all tables in this database
GRANT SELECT ON ALL TABLES in schema customer_leads.public TO SHARE share_leads_databases;

-- allow consumer account access on the Share
-- to find the consumer_account_number look at the URL of the snowflake
-- instance of the consumer. So if the URL is https://jy80556.ap-southeast-2.snowflakecomputing.com/console/login
-- the consumer account_number is jy80556
ALTER SHARE share_leads_databases ADD ACCOUNT=consumer_account_number_here;



-- On the consumer create a database from the share
CREATE DATABASE shared_prospects FROM SHARE producer_account_number_here.share_leads_databases;
