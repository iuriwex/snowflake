-- logged in as the marketing DBA create new table
-- and grant access to the marketing analyst role

-- you would be able to do so since the marketing dba role 
-- owns the MARKETING_DATABASE

CREATE TABLE MARKETING_DATABASE.PUBLIC.CUSTOMER
(
  FIRSTNAME STRING,
  LASTNAME STRING
);

-- Grant usage on the database and the schema
GRANT USAGE ON DATABASE MARKETING_DATABASE TO ROLE MARKETING_ANALYST;
GRANT USAGE ON SCHEMA MARKETING_DATABASE.PUBLIC TO ROLE MARKETING_ANALYST;

-- Grant select on the table
GRANT SELECT ON TABLE MARKETING_DATABASE.PUBLIC.CUSTOMER TO ROLE MARKETING_ANALYST;

