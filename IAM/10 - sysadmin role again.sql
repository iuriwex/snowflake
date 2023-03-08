-- Login as Jane who is the SYSADMIN and

-- select from the finance transaction table which is not possible
-- as the role hierarchy isn't complete due to the SYSADMIN doesn't 
-- have the FINANCE_DBA role
SELECT * FROM FINANCE_DATABASE.PUBLIC.TRANSACTIONS;