-- grant USAGE to a database to PUBLIC role
-- we will use this database to demonstrate queries
GRANT USAGE ON DATABASE INGEST_DATA TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA INGEST_DATA.PUBLIC TO ROLE PUBLIC;
GRANT SELECT ON TABLE INGEST_DATA.PUBLIC.TRANSACTIONS TO ROLE PUBLIC;

-- virtual warehouse for demonstrating multiclsuter
CREATE WAREHOUSE multicluster_wh WITH WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;


CREATE ROLE stress_test;
GRANT USAGE ON WAREHOUSE multicluster_wh TO ROLE stress_test; 


-- create a login for data scientist & data scientist 2
CREATE USER ST_1 PASSWORD = 'ST_1' LOGIN_NAME = 'ST_1' DEFAULT_ROLE = "stress_test" DEFAULT_WAREHOUSE = 'multicluster_wh' MUST_CHANGE_PASSWORD = FALSE;
CREATE USER ST_2 PASSWORD = 'ST_2' LOGIN_NAME = 'ST_2' DEFAULT_ROLE = "stress_test" DEFAULT_WAREHOUSE = 'multicluster_wh' MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE stress_test TO USER ST_1;
GRANT ROLE stress_test TO USER ST_2;

-- Alter the virtual warehouse set min/max cluster count & also the sclaing policy
ALTER WAREHOUSE multicluster_wh SET MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'ECONOMY';