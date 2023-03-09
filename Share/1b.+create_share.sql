-- create the share object to which we will then add data to be shared. 
CREATE SHARE share_customer_data;

-- grant usage on the database in which our table is contained
-- this step is necessary to subsequently provide access to the table
GRANT USAGE ON DATABASE crm_data TO SHARE share_customer_data;

-- grant usage on the schema in which our table is contained.
-- again this step is necessary to subsequently provide access to the table
GRANT USAGE ON SCHEMA crm_data.public TO SHARE share_customer_data;

-- add the customer table to the share
-- We have provided only SELECT on the shared table so the consumer can 
-- only read the data but will not be able to modify
GRANT SELECT ON TABLE crm_data.public.customer TO SHARE share_customer_data;

-- Validate what objects does the share has access to
SHOW GRANTS TO SHARE share_customer_data;

-- allow consumer account access on the Share
-- to find the consumer_account_number look at the URL of the snowflake
-- instance of the consumer. So if the URL is https://jy80556.ap-southeast-2.snowflakecomputing.com/console/login
-- the consumer account_number is jy80556
ALTER SHARE share_customer_data ADD ACCOUNT=consumer_account_number_here;


-- Find who has been granted access to the share
SHOW GRANTS OF SHARE share_customer_data;
