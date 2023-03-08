CREATE DATABASE reporting_views;

-- Creat the view as secure view as only secure views can be shared
CREATE SECURE VIEW customer_by_state
(
STATE,
CUSTOMER_COUNT
)
AS
SELECT STATE,COUNT(*) AS CUSTOMER_COUNT FROM crm_data.public.CUSTOMER
GROUP BY STATE;

-- test that the view works
SELECT * FROM customer_by_state;


-- create the share object to which we will then add the view to be shared. 
CREATE SHARE share_customer_analysis;


-- grant usage on the database in which our view is contained
-- this step is necessary to subsequently provide access to the view
GRANT USAGE ON DATABASE reporting_views TO SHARE share_customer_analysis;

-- grant usage on the schema in which our table is contained.
-- again this step is necessary to subsequently provide access to the table
GRANT USAGE ON SCHEMA reporting_views.public TO SHARE share_customer_analysis;

-- grant reference_usage on the database(s) in which the tables referenced
-- in view SQL are cotained. So in this case we will grant reference_usage
-- to crm_data database since our table is contained in it
GRANT REFERENCE_USAGE ON DATABASE crm_data TO SHARE share_customer_analysis;

-- add the customer table to the share
-- We have provided only SELECT on the shared table so the consumer can 
-- only read the data but will not be able to modify
GRANT SELECT ON VIEW reporting_views.public.customer_by_state TO SHARE share_customer_analysis;


-- allow consumer account access on the Share
-- to find the consumer_account_number look at the URL of the snowflake
-- instance of the consumer. So if the URL is https://jy80556.ap-southeast-2.snowflakecomputing.com/console/login
-- the consumer account_number is jy80556
ALTER SHARE share_customer_analysis ADD ACCOUNT=<consumer_account_number>;