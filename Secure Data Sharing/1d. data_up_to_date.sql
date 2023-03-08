-- Run this on the consumer to select one row
SELECT * FROM CUSTOMER WHERE NAME LIKE '%John%';
-- note that the Post Code is 2099

-- On the producer let's update the data 
UPDATE CUSTOMER SET PostCode = 2100 WHERE NAME LIKE '%John%';

-- Run this on the consumer to select one row
SELECT * FROM CUSTOMER WHERE NAME LIKE '%John%';
-- note that the Post Code is 2100 which is the update postcode