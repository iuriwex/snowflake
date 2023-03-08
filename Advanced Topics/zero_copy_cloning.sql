select * from customer;
select count(*) from customer;

-- clone our customer table
create table customer_copy clone customer;

-- validate data is similar
select * from customer_copy;
select count(*) from customer_copy;

UPDATE customer_copy set JOB = 'Snowflake Architect' where NAME = 'Jeffrey Garrett';

-- validate data changes are tracked independantly in both tables
select * from customer_copy;
select * from customer;



-- drop the cloned table so that it doesn't cause confusion
drop table customer_copy;
--clone the public schema
create schema public_copy clone public;


--clone the prod_crm schema
create database dev_crm clone prod_crm;