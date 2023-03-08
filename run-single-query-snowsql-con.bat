@echo off
set SNOWSQL_PWD=''
@echo on 
snowsql -a rhgfdbp-zp79074.snowflakecomputing.com -u SFADMINUSER -w AS_WH -s TEST_SCALE_OUT -f snowflake_tutorial.sql
 