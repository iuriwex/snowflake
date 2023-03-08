-- Login as Jane who is the SYSADMIN & try to access the 
-- customer table in the marketing database
-- the user would be able to access because of the role hierarchy 
-- as Jane also has the MARKETING_DBA role

-- SELECT from the CUSTOMER table
SELECT * FROM MARKETING_DATABASE.PUBLIC.CUSTOMER;

-- Try creating a new table in the MARKETING DATABASE
CREATE TABLE MARKETING_DATABASE.PUBLIC.TEST_NEW_TABLE
(
ID STRING
);

-- Drop an existing table in the marketing database
DROP TABLE MARKETING_DATABASE.PUBLIC.CUSTOMER;