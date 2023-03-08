-- Assuming that we have to share data with a non-snowflake customer
-- we will first create a new reader account for the non-snowflake user
CREATE MANAGED ACCOUNT cliff_inc_account
ADMIN_NAME = cliffincadmin , ADMIN_PASSWORD = 'ShinyNewP@ssword123' ,
TYPE = READER;

-- The above statement onces it successfully runs will show you the account 
-- & the URL for the newly created account


-- See which reader accounts exist
-- you can also use this command to see the URL through which a reader
-- should access the data
SHOW MANAGED ACCOUNTS;

-- Share the data with the reader account
-- We will use the existing share that we created earlier in the lab
-- find the account name using the above command and then use that in the statement below.
-- the account name or number can be found under the locator field when running SHOW MANAGED ACCOUNTS;
ALTER SHARE share_customer_data ADD ACCOUNT=<consumer_account_number>;