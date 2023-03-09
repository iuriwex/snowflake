-- next we must login into the target reader account and set it up
CREATE USER john PASSWORD = 'Test123!' MUST_CHANGE_PASSWORD = FALSE;

-- create a database in reader snowflake instance based on the share.
CREATE DATABASE MYCUSTOMER FROM SHARE <provider_account_number>.SHARE_CUSTOMER_DATA;

-- grant privileges on MYCUSTOMER database to the public role 
GRANT IMPORTED PRIVILEGES ON DATABASE MYCUSTOMER TO ROLE public;

-- create virtual warehouse
CREATE WAREHOUSE miniVWH WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE miniVWH TO ROLE public;