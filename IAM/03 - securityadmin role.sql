-- We will loging as user john who has the SECURITYADMIN role


-- let's start by creating a bunch of roles & users that we will use further
-- refer Roles hands on - outline 


-- Create a role for the marketing database administrators
-- people in this role will be able to create manage objects
-- in the marketing database
CREATE ROLE MARKETING_DBA;


-- Create a role for the marketing analyst users
CREATE ROLE MARKETING_ANALYST;

-- Grant the MARKETING_ANALYST role to the MARKETING_DBA
-- as per the guidlines by snowflake i.e. to connect the
-- hierarchy of roles
GRANT ROLE MARKETING_ANALYST TO ROLE MARKETING_DBA;

-- Grant the MARKETING_DBA role to the SYSADMIN
-- as per the guidlines by snowflake i.e. to connect the
-- hierarchy of roles
GRANT ROLE MARKETING_DBA TO ROLE SYSADMIN;


-- create a marketing analyst
CREATE USER mkt_user_1 PASSWORD = 'Johndoe123' DEFAULT_ROLE = MARKETING_ANALYST MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE MARKETING_ANALYST TO USER mkt_user_1;

-- create a marketing database administrator
CREATE USER mkt_dba_1 PASSWORD = 'Johndoe123' DEFAULT_ROLE = MARKETING_DBA MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE MARKETING_DBA TO USER mkt_dba_1;


-- let's do the same activities for finance users as well
-- except we won't connect the finance DBA to the SYSADMIN 
-- in order to demonstrate the ramifications of not connecting

-- Create a role for the finance database administrators
-- people in this role will be able to create manage objects
-- in the finance database
CREATE ROLE FINANCE_DBA;


-- Create a role for the finance analyst users
CREATE ROLE FINANCE_ANALYST;

-- Grant the FINANCE_ANALYST role to the FINANCE_DBA
-- as per the guidlines by snowflake i.e. to connect the
-- hierarchy of roles
GRANT ROLE FINANCE_ANALYST TO ROLE FINANCE_DBA;

-- DO NOT Grant the FINANCE_DBA role to the SYSADMIN
-- ============================
-- as a result object created by the FINANCE_DBA can't
-- be managed by the SYSADMIN


-- create a finance analyst
CREATE USER fin_user_1 PASSWORD = 'Johndoe123' DEFAULT_ROLE = FINANCE_ANALYST MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE FINANCE_ANALYST TO USER fin_user_1;

-- create a finance database administrator
CREATE USER fin_dba_1 PASSWORD = 'Johndoe123' DEFAULT_ROLE = FINANCE_DBA MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE FINANCE_DBA TO USER fin_dba_1;


