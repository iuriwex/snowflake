
-- need to use SECURITYADMIN role to create new roles
USE ROLE SECURITYADMIN;
CREATE ROLE marketing_users;

-- create a test user who has default role set to marketing_users
CREATE USER mkt_1 PASSWORD = 'doe' DEFAULT_ROLE = "marketing_users" MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE marketing_users TO USER mkt_1;

-- switch role to SYSADMIN & create the database and tables
USE ROLE SYSADMIN;
CREATE DATABASE Marketing_Data;

USE Marketing_Data;
CREATE TABLE customer
(
	Customer_ID Number,
	Customer_Name String
);

-- since SYSDADMIN role is the owner it can grant access to the customer table
GRANT SELECT ON customer to marketing_users;

--- let's attempt to SELECT from customer as mkt_1 now

USE ROLE SYSADMIN;

-- grant the required USAGE privileges
GRANT USAGE ON DATABASE Marketing_Data to marketing_users;
GRANT USAGE ON SCHEMA Marketing_Data.public to marketing_users;

--- let's attempt to SELECT from customer as mkt_1 again

USE ROLE SYSADMIN;
USE Marketing_Data;
-- let's create a new table
CREATE TABLE transactions
(
	Customer_ID Number,
	Transaction_ID Number,
	Amount Number
);

--- let's attempt to SELECT from transactions as mkt_1

USE ROLE SYSADMIN;
USE Marketing_Data;

CREATE TABLE complaints
(
	Customer_ID Number,
	Comlaint_Id Number
);

GRANT SELECT ON ALL TABLES IN DATABASE Marketing_Data to marketing_users;
--- let's attempt to SELECT from transactions as mkt_1

-- let's create a new table called address
USE ROLE SYSADMIN;
USE Marketing_Data;

CREATE TABLE address
(
	Customer_ID Number,
	Address_Id Number
);


--switch to SECURITYADMIN role to run the grant on FUTURE role
USE ROLE SECURITYADMIN;
GRANT SELECT ON FUTURE TABLES IN DATABASE Marketing_Data to marketing_users;


USE ROLE SYSADMIN;
USE Marketing_Data;
-- let's create a new table and check if mkt_1 can select from this table
CREATE TABLE city
(
	City_ID Number,
	City_Name String
);
