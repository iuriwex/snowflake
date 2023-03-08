create or replace file format file_format_def
  field_delimiter = none
  record_delimiter = '\\n';

-- create an external stage using the S3 bucket that contains JSON data
CREATE OR REPLACE STAGE json_example_stage url='s3://snowflake-essentials/json_data'
file_format = file_format_def;

CREATE TABLE tableX (
    Company STRING,
    State   STRING,
    OrganisationCOde STRING
); 

copy into tableX(Company, State, OrganisationCode)
   from (select substr(parse_json($1):organisations.Company,4), substr(parse_json($1):organisations.Company,1,2), parse_json($1):organisations.OrganisationCode
   from @json_example_stage/organisations.json t)
   on_error = 'continue';
   
   
   select parse_json($1):organisations:Company
   from @json_example_stage/organisations_1.json t