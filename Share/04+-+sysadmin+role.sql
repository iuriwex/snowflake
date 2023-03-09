-- we will perform activities in this script using the user Jane 
-- that we earlier created who has the SYSADMIN role


-- let's create a new small size virtual warehouse 
CREATE WAREHOUSE COMMON_WH WITH 
WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

-- and grant it to the PUBLIC role so that any one can make use of it
GRANT USAGE ON WAREHOUSE COMMON_WH TO ROLE PUBLIC;



-- let's also create a new database for storing temporary data and grant it to PUBLIC role
CREATE DATABASE temp_database;
GRANT USAGE ON DATABASE temp_database TO ROLE PUBLIC;




-- create a database where we will have all the marketing 
-- tables and grant access to this database to the MARKETING_DBA;
CREATE DATABASE MARKETING_DATABASE;
GRANT OWNERSHIP ON SCHEMA MARKETING_DATABASE.PUBLIC TO ROLE MARKETING_DBA;
GRANT OWNERSHIP ON DATABASE MARKETING_DATABASE TO ROLE MARKETING_DBA;




-- create a database where we will have all the finance 
-- tables and grant access to this database to the FINANCE_DBA;
CREATE DATABASE FINANCE_DATABASE;
GRANT OWNERSHIP ON SCHEMA FINANCE_DATABASE.PUBLIC TO ROLE FINANCE_DBA;
GRANT OWNERSHIP ON DATABASE FINANCE_DATABASE TO ROLE FINANCE_DBA;