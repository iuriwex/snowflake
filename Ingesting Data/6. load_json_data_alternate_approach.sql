-- create a database if it doesn't already exist
CREATE DATABASE ingest_data;

USE DATABASE ingest_data;

-- specify the file format which has no field delimiter as we are expecting 
-- a complete JSON per line & the record_delimiter is the new line character
-- so basically one valid JSON row per line
CREATE OR REPLACE file format file_format_def
  field_delimiter = none
  record_delimiter = '\\n';

-- create an external stage using the S3 bucket that contains JSON data
CREATE OR REPLACE STAGE json_example_stage url='s3://snowflake-essentials/json_data'
file_format = file_format_def;

-- create the target table
CREATE TABLE organisations_direct_load (
    org_name STRING,
    state   STRING,
    org_code STRING,
	extract_date DATE
); 

-- load the JSON from the stage & select a basic attribute
SELECT  parse_json($1):extract_date
FROM @json_example_stage/simple_for_load.json;

-- select the organisation attribute which itself is nested JSON
SELECT  parse_json($1):organisation,
        parse_json($1):extract_date
FROM @json_example_stage/simple_for_load.json;

-- select the nested attributes for the organisation
SELECT  parse_json($1):organisation:name,
        parse_json($1):organisation:state,
        parse_json($1):organisation:org_code,
        parse_json($1):extract_date
FROM @json_example_stage/simple_for_load.json;


-- COPY into organisations_direct_load table
COPY INTO organisations_direct_load(org_name, state, org_code, extract_date)
FROM(
    SELECT  parse_json($1):organisation:name,
        parse_json($1):organisation:state,
        parse_json($1):organisation:org_code,
        parse_json($1):extract_date
    FROM @json_example_stage/simple_for_load.json
);
   
-- validate load is successful
SELECT * FROM organisations_direct_load;