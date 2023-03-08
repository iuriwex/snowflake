
-- create an external stage using the S3 bucket that we created in the last step
create or replace stage bulk_copy_example_stage url='s3://snowflake-essentials/ingesting_data/new_customer';

-- list the files in the bucket
list @bulk_copy_example_stage;

use database ingest_data;

-- bulk copy into the customer table using the stage
-- specify the file formate and the delimiter , which in our case was the pipe character
-- also specify the pattern to look for specific files. Please note that the pattern is a regex expression.
-- the pattern is not the wild card expressions commonly used on the shell
copy into customer
  from @bulk_copy_example_stage
  pattern='.*.csv'
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT COUNT(*) FROM customer;
--should return 100 rows

-- repeate the load command again
-- nothing should get loaded and the row count should remain 100
-- this is because snowflake tracks data load for a period of time
-- and will not load files that are already loaded
copy into customer
  from @bulk_copy_example_stage
  pattern='.*.csv'
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT COUNT(*) FROM customer;
--should return 100 rows as no new data is loaded
--since the data was already processed



copy into customer
  from @bulk_copy_example_stage/2019-09-24/additional_data.txt
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT COUNT(*) FROM customer;
--should return 200 rows since we have now loaded additional data
