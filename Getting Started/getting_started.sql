CREATE DATABASE OUR_FIRST_DATABASE;

CREATE TABLE OUR_FIRST_TABLE (
  first_name STRING ,
  last_name STRING ,
  address string ,
  city string ,
  state string
  );

  
--sample data for the table above is present in AWS S3 at
https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/our_first_table_data.csv

create or replace stage my_s3_stage url='s3://snowflake-essentials/';


copy into OUR_FIRST_TABLE
  from s3://snowflake-essentials/our_first_table_data.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM OUR_FIRST_TABLE;

SELECT COUNT(*) FROM OUR_FIRST_TABLE;