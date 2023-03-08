ALTER SESSION SET TIMEZONE = 'UTC';

CREATE DATABASE PROD_CRM;

CREATE TABLE CUSTOMER (
  Name STRING ,
  Email STRING ,
  Job STRING ,
  Phone STRING ,
  Age NUMBER
  );
  
  
--sample data for the table above is present in AWS S3 at
https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/sample_data_for_cloning.csv

create or replace stage my_s3_stage url='s3://snowflake-essentials/';


copy into CUSTOMER
  from s3://snowflake-essentials/sample_data_for_cloning.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM CUSTOMER;

-- select the current time stamp so that we can use it in the time travel query
SELECT CURRENT_TIMESTAMP;

--2019-05-26 03:20:15.855

-- update the Job column setting all rows to the same value
update CUSTOMER set Job = 'Snowflake Architect';

SELECT * FROM CUSTOMER;

-- time travel to a time just before the update was run
select * from CUSTOMER before(timestamp => '2019-05-26 03:20:15.855'::timestamp);

-- time travel to 10 minutes ago (i.e. before we ran the update)
select * from CUSTOMER AT(offset => -60*10);

-- note down the query id of this query as we will use it in the time travel query as well
update CUSTOMER set Job = NULL;
--018c6f1f-00fd-b06c-0000-00000e99c991

-- time travel to the time before the update query was run
select * from CUSTOMER before(statement => '018c6f1f-00fd-b06c-0000-00000e99c991');



