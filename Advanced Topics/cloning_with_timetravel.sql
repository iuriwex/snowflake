ALTER SESSION SET TIMEZONE = 'UTC';

-- select the current time stamp so that we can use it in the time travel query
SELECT CURRENT_TIMESTAMP;

--sample data for the table above is present in AWS S3 at
https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/sample_data_for_cloned_timetravel.csv

create or replace stage my_s3_stage url='s3://snowflake-essentials/';

-- load additional data into the customer table
copy into CUSTOMER
  from s3://snowflake-essentials/sample_data_for_cloned_timetravel.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);
  
  
select count(*) from CUSTOMER;

-- clone our customer table
create table CUSTOMER_dev_b4_time clone customer before(timestamp => ''::timestamp);

-- clone our customer table
create table CUSTOMER_dev_b4_insert clone customer (statement => '(statement => '');');


