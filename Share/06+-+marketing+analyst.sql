-- login as mkt_user_1 and validate
-- you are able to select form the customer 
-- table. 
-- make sure you select the correct database on the worksheet
-- before running the query

-- the queries will run so you have the 
-- privileges (although zero rows as we did not load the table)
SELECT * FROM CUSTOMER;
SELECT COUNT(*) FROM CUSTOMER;


-- Try deleting from the table,
-- it should fail as you don't have 
-- privileges to do so
DELETE FROM CUSTOMER;