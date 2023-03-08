@echo off
set SNOWSQL_PWD=replace_with_your_password
@echo on
snowsql -a replace_with_your_account_identifier -u replace_with_your_user -w AS_WH -d TEST_SCALE_OUT -s PUBLIC -f 03a_complex_query.sql