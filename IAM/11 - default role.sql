-- We will loging as user john who has the SECURITYADMIN role
-- and can create new users


CREATE USER alpha PASSWORD = 'bravo' DEFAULT_ROLE = SYSADMIN;
-- now log out and login as the user alpha


-- login back as John to grant the SYSADMIN role to alpha
GRANT ROLE SYSADMIN TO USER alpha;



