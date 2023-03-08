-- login into the consumer snowflake isntance as the admin account for that instance
-- switch your role to account admin
-- replace the producer_account_number below with your account number
-- and create a database from the share.
CREATE DATABASE my_customer_analytics from SHARE producer_account_number.share_customer_analysis;

-- select from the view
SELECT * FROM my_customer_analytics.public.customer_by_state;

