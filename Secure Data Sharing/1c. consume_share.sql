-- List the inbound and outbound shares that are currently present in the system
SHOW SHARES;

-- Find the share details by running describe. 
-- Always use provider_account.share_name
DESC SHARE <provider_account_number>.SHARE_CUSTOMER_DATA;

-- create a database in consumer snowflake instance based on the share.
CREATE DATABASE CUSTOMER_DATABASE FROM SHARE <provider_account_number>.SHARE_CUSTOMER_DATA;

-- select from the shared table
SELECT * FROM CUSTOMER_DATABASE.PUBLIC.CUSTOMER;