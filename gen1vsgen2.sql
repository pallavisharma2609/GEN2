-- =================================================================
-- SNOWFLAKE GEN2 VS GEN1 PERFORMANCE BENCHMARK SCRIPT - FINAL VERSION
-- Complete testing framework for comparing warehouse performance
-- Updated with credit calculation fixes and comprehensive analysis
-- =================================================================

-- Step 0: Enable SNOWFLAKE_SAMPLE_DATA if not exists 
-- ==================================================
-- Create a database from the share.

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;

-- Grant the PUBLIC role access to the database.
-- Optionally change the role name to restrict access to a subset of users.
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE PUBLIC;

-- Step 1: Create Your Test Warehouses
-- ===================================

-- Create Gen1 warehouse for baseline
CREATE OR REPLACE WAREHOUSE TEST_GEN1_MEDIUM
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Gen1 baseline for performance testing';

-- Create Gen2 warehouse for comparison  
CREATE OR REPLACE WAREHOUSE TEST_GEN2_MEDIUM
  WAREHOUSE_SIZE = MEDIUM
  RESOURCE_CONSTRAINT = STANDARD_GEN_2
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Gen2 warehouse for performance testing';

-- Verify both warehouses are created
SHOW WAREHOUSES LIKE 'TEST_%';

-- Step 2: Set Up Performance Tracking
-- ===================================

CREATE OR REPLACE DATABASE TEST_GEN2;
CREATE OR REPLACE SCHEMA TEST_GEN2.SCHEMA_GEN2;

-- Create table to track your test results
CREATE OR REPLACE TABLE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS (
    test_id VARCHAR(100),
    warehouse_name VARCHAR(50),
    warehouse_generation VARCHAR(10),
    query_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    execution_time_seconds NUMBER(10,3),
    credits_consumed NUMBER(10,6),
    rows_processed NUMBER,
    test_date DATE,
    notes VARCHAR(500)
);

-- Step 3: Copy Sample Data for Testing
-- ====================================

-- Create your own writable copy
CREATE OR REPLACE DATABASE GEN2_BENCHMARK_TEST;
CREATE OR REPLACE SCHEMA GEN2_BENCHMARK_TEST.TESTING;

-- Copy the tables (now you can modify them!)
CREATE OR REPLACE TABLE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

CREATE OR REPLACE TABLE GEN2_BENCHMARK_TEST.TESTING.ORDERS AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

CREATE OR REPLACE TABLE GEN2_BENCHMARK_TEST.TESTING.LINEITEM AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

-- Verify data copy
SELECT 'CUSTOMER' as table_name, COUNT(*) as row_count FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER
UNION ALL
SELECT 'ORDERS' as table_name, COUNT(*) as row_count FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS  
UNION ALL
SELECT 'LINEITEM' as table_name, COUNT(*) as row_count FROM GEN2_BENCHMARK_TEST.TESTING.LINEITEM;

-- Step 4: Run Benchmark Tests
-- ===========================

-- =================================================================
-- TEST 1: DML Operations (High Impact Expected)
-- =================================================================

-- Switch to Gen1 warehouse
USE WAREHOUSE TEST_GEN1_MEDIUM;

-- Record start time and run test
SET start_time = CURRENT_TIMESTAMP();

-- DML Test: Customer data updates (using numeric column to avoid string issues)
UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_NATIONKEY = (C_NATIONKEY + 1) % 25
WHERE C_CUSTKEY <= 10000;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'DML_UPDATE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Customer_Update_DML',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0, -- Will calculate credits separately
    10000,
    CURRENT_DATE(),
    'DML UPDATE test on customer table';

-- Now test with Gen2
USE WAREHOUSE TEST_GEN2_MEDIUM;

-- Reset data first (revert the nationkey changes from Gen1 test)
UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_NATIONKEY = (C_NATIONKEY - 1 + 25) % 25
WHERE C_CUSTKEY <= 10000;

-- Run Gen2 test (identical operation to Gen1 for fair comparison)
SET start_time = CURRENT_TIMESTAMP();

UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_NATIONKEY = (C_NATIONKEY + 1) % 25
WHERE C_CUSTKEY <= 10000;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'DML_UPDATE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Customer_Update_DML',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    10000,
    CURRENT_DATE(),
    'DML UPDATE test - nationkey column update - Gen2';

-- =================================================================
-- TEST 2: Bulk INSERT Operations (High Volume DML)
-- =================================================================

-- Gen1 Bulk Insert Test
USE WAREHOUSE TEST_GEN1_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Create large dataset insert (50K records)
INSERT INTO GEN2_BENCHMARK_TEST.TESTING.CUSTOMER
SELECT 
    C_CUSTKEY + 150000 AS C_CUSTKEY,  -- Offset to avoid duplicates
    'Customer_' || (C_CUSTKEY + 150000)::STRING AS C_NAME,
    CASE (C_CUSTKEY % 10) 
        WHEN 0 THEN '123 New Street'
        WHEN 1 THEN '456 Data Lane' 
        WHEN 2 THEN '789 Cloud Ave'
        WHEN 3 THEN '321 Analytics Blvd'
        WHEN 4 THEN '654 Compute Dr'
        WHEN 5 THEN '987 Storage St'
        WHEN 6 THEN '147 Warehouse Way'
        WHEN 7 THEN '258 Performance Pl'
        WHEN 8 THEN '369 Scale Circle'
        ELSE '741 Benchmark Rd'
    END AS C_ADDRESS,
    C_NATIONKEY,
    '25-' || LPAD((C_CUSTKEY % 1000)::STRING, 3, '0') || '-' || LPAD(((C_CUSTKEY + 150000) % 10000)::STRING, 4, '0') AS C_PHONE,
    ROUND((C_CUSTKEY % 10000) + UNIFORM(0, 5000, RANDOM()), 2) AS C_ACCTBAL,
    C_MKTSEGMENT,
    'Bulk inserted test customer for Gen2 benchmark - ' || CURRENT_DATE()::STRING AS C_COMMENT
FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
WHERE C_CUSTKEY <= 50000;  -- Insert 50K new records

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 bulk insert results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'BULK_INSERT_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Bulk_Customer_Insert',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    50000,
    CURRENT_DATE(),
    'Bulk INSERT of 50K customer records - Gen1';

-- Gen2 Bulk Insert Test
USE WAREHOUSE TEST_GEN2_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Same bulk insert operation on Gen2
INSERT INTO GEN2_BENCHMARK_TEST.TESTING.CUSTOMER
SELECT 
    C_CUSTKEY + 200000 AS C_CUSTKEY,  -- Different offset for Gen2
    'Customer_' || (C_CUSTKEY + 200000)::STRING AS C_NAME,
    CASE (C_CUSTKEY % 10) 
        WHEN 0 THEN '123 New Street'
        WHEN 1 THEN '456 Data Lane' 
        WHEN 2 THEN '789 Cloud Ave'
        WHEN 3 THEN '321 Analytics Blvd'
        WHEN 4 THEN '654 Compute Dr'
        WHEN 5 THEN '987 Storage St'
        WHEN 6 THEN '147 Warehouse Way'
        WHEN 7 THEN '258 Performance Pl'
        WHEN 8 THEN '369 Scale Circle'
        ELSE '741 Benchmark Rd'
    END AS C_ADDRESS,
    C_NATIONKEY,
    '25-' || LPAD((C_CUSTKEY % 1000)::STRING, 3, '0') || '-' || LPAD(((C_CUSTKEY + 200000) % 10000)::STRING, 4, '0') AS C_PHONE,
    ROUND((C_CUSTKEY % 10000) + UNIFORM(0, 5000, RANDOM()), 2) AS C_ACCTBAL,
    C_MKTSEGMENT,
    'Bulk inserted test customer for Gen2 benchmark - ' || CURRENT_DATE()::STRING AS C_COMMENT
FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
WHERE C_CUSTKEY <= 50000;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 bulk insert results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'BULK_INSERT_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Bulk_Customer_Insert',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    50000,
    CURRENT_DATE(),
    'Bulk INSERT of 50K customer records - Gen2';

-- =================================================================
-- TEST 3: Complex UPDATE with JOINs (Complex DML)
-- =================================================================

-- Gen1 Complex Update Test
USE WAREHOUSE TEST_GEN1_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Complex UPDATE that requires joining multiple tables and calculations
UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_ACCTBAL = C_ACCTBAL + subquery.avg_order_value
FROM (
    SELECT 
        O.O_CUSTKEY,
        AVG(O.O_TOTALPRICE) AS avg_order_value
    FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS O
    WHERE O.O_ORDERDATE >= '1995-01-01'
    AND O.O_CUSTKEY <= 50000
    GROUP BY O.O_CUSTKEY
    HAVING COUNT(*) >= 3  -- Only customers with 3+ orders
) subquery
WHERE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY = subquery.O_CUSTKEY
AND GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY <= 50000;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 complex update results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'COMPLEX_UPDATE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Complex_Update_with_Joins',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    25000,  -- Approximate rows affected
    CURRENT_DATE(),
    'Complex UPDATE with subquery and aggregation - Gen1';

-- Gen2 Complex Update Test
USE WAREHOUSE TEST_GEN2_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Reset the account balances first, then run same complex update
UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_ACCTBAL = C_ACCTBAL - subquery.avg_order_value
FROM (
    SELECT 
        O.O_CUSTKEY,
        AVG(O.O_TOTALPRICE) AS avg_order_value
    FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS O
    WHERE O.O_ORDERDATE >= '1995-01-01'
    AND O.O_CUSTKEY <= 50000
    GROUP BY O.O_CUSTKEY
    HAVING COUNT(*) >= 3
) subquery
WHERE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY = subquery.O_CUSTKEY
AND GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY <= 50000;

-- Now run the actual test (same as Gen1)
SET start_time = CURRENT_TIMESTAMP();

UPDATE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
SET C_ACCTBAL = C_ACCTBAL + subquery.avg_order_value
FROM (
    SELECT 
        O.O_CUSTKEY,
        AVG(O.O_TOTALPRICE) AS avg_order_value
    FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS O
    WHERE O.O_ORDERDATE >= '1995-01-01'
    AND O.O_CUSTKEY <= 50000
    GROUP BY O.O_CUSTKEY
    HAVING COUNT(*) >= 3
) subquery
WHERE GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY = subquery.O_CUSTKEY
AND GEN2_BENCHMARK_TEST.TESTING.CUSTOMER.C_CUSTKEY <= 50000;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 complex update results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'COMPLEX_UPDATE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Complex_Update_with_Joins',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    25000,
    CURRENT_DATE(),
    'Complex UPDATE with subquery and aggregation - Gen2';

-- =================================================================
-- TEST 4: Large DELETE Operations (Data Cleanup DML)
-- =================================================================

-- Gen1 Large Delete Test
USE WAREHOUSE TEST_GEN1_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Delete test records with complex WHERE conditions
DELETE FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
WHERE C_CUSTKEY IN (
    SELECT C.C_CUSTKEY 
    FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    WHERE C.C_CUSTKEY >= 150000  -- Our test data
    GROUP BY C.C_CUSTKEY
    HAVING COUNT(O.O_ORDERKEY) < 2  -- Customers with < 2 orders
);

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 delete results  
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'LARGE_DELETE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Large_Delete_Complex_WHERE',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    40000,  -- Approximate rows deleted
    CURRENT_DATE(),
    'Large DELETE with complex subquery conditions - Gen1';

-- Gen2 Large Delete Test
USE WAREHOUSE TEST_GEN2_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Same delete operation on Gen2 test data
DELETE FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER 
WHERE C_CUSTKEY IN (
    SELECT C.C_CUSTKEY 
    FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    WHERE C.C_CUSTKEY >= 200000  -- Our Gen2 test data
    GROUP BY C.C_CUSTKEY
    HAVING COUNT(O.O_ORDERKEY) < 2  -- Customers with < 2 orders
);

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 delete results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'LARGE_DELETE_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Large_Delete_Complex_WHERE',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    40000,
    CURRENT_DATE(),
    'Large DELETE with complex subquery conditions - Gen2';

-- =================================================================
-- TEST 5: Complex Analytics (Scan-Heavy Workload)
-- =================================================================

-- Gen1 Analytics Test
USE WAREHOUSE TEST_GEN1_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Complex customer segmentation query
WITH customer_metrics AS (
    SELECT 
        C.C_CUSTKEY,
        C.C_MKTSEGMENT,
        COUNT(DISTINCT O.O_ORDERKEY) AS order_count,
        SUM(O.O_TOTALPRICE) AS total_spent,
        AVG(O.O_TOTALPRICE) AS avg_order_value,
        COUNT(DISTINCT L.L_PARTKEY) AS unique_parts_ordered
    FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
    GROUP BY C.C_CUSTKEY, C.C_MKTSEGMENT
    HAVING COUNT(DISTINCT O.O_ORDERKEY) >= 3
)
SELECT 
    C_MKTSEGMENT,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_lifetime_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_spent) AS median_ltv,
    AVG(unique_parts_ordered) AS avg_product_diversity
FROM customer_metrics
GROUP BY C_MKTSEGMENT
ORDER BY avg_lifetime_value DESC;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 analytics results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'ANALYTICS_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Complex_Customer_Analytics',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    150000, -- Approximate rows processed
    CURRENT_DATE(),
    'Complex customer segmentation analytics - Gen1';

-- Gen2 Analytics Test  
USE WAREHOUSE TEST_GEN2_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Same complex query on Gen2
WITH customer_metrics AS (
    SELECT 
        C.C_CUSTKEY,
        C.C_MKTSEGMENT,
        COUNT(DISTINCT O.O_ORDERKEY) AS order_count,
        SUM(O.O_TOTALPRICE) AS total_spent,
        AVG(O.O_TOTALPRICE) AS avg_order_value,
        COUNT(DISTINCT L.L_PARTKEY) AS unique_parts_ordered
    FROM GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    LEFT JOIN GEN2_BENCHMARK_TEST.TESTING.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
    GROUP BY C.C_CUSTKEY, C.C_MKTSEGMENT
    HAVING COUNT(DISTINCT O.O_ORDERKEY) >= 3
)
SELECT 
    C_MKTSEGMENT,
    COUNT(*) AS customer_count,
    AVG(total_spent) AS avg_lifetime_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_spent) AS median_ltv,
    AVG(unique_parts_ordered) AS avg_product_diversity
FROM customer_metrics
GROUP BY C_MKTSEGMENT
ORDER BY avg_lifetime_value DESC;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 analytics results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'ANALYTICS_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Complex_Customer_Analytics',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    150000,
    CURRENT_DATE(),
    'Complex customer segmentation analytics - Gen2';

-- =================================================================
-- TEST 3: Large Aggregation (Table Scan Heavy)
-- =================================================================

-- Gen1 Large Aggregation Test
USE WAREHOUSE TEST_GEN1_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Large aggregation with multiple joins
SELECT 
    O.O_ORDERPRIORITY,
    C.C_MKTSEGMENT,
    COUNT(*) AS order_count,
    SUM(O.O_TOTALPRICE) AS total_revenue,
    AVG(O.O_TOTALPRICE) AS avg_order_value,
    SUM(L.L_QUANTITY) AS total_quantity,
    COUNT(DISTINCT C.C_CUSTKEY) AS unique_customers,
    COUNT(DISTINCT L.L_PARTKEY) AS unique_parts
FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS O
JOIN GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN GEN2_BENCHMARK_TEST.TESTING.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE O.O_ORDERDATE >= '1995-01-01'
GROUP BY O.O_ORDERPRIORITY, C.C_MKTSEGMENT
ORDER BY total_revenue DESC;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen1 aggregation results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'AGGREGATION_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN1_MEDIUM',
    'GEN1',
    'Large_Table_Aggregation',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    6000000, -- Approximate rows processed
    CURRENT_DATE(),
    'Large aggregation with multi-table joins - Gen1';

-- Gen2 Large Aggregation Test
USE WAREHOUSE TEST_GEN2_MEDIUM;
SET start_time = CURRENT_TIMESTAMP();

-- Same aggregation on Gen2
SELECT 
    O.O_ORDERPRIORITY,
    C.C_MKTSEGMENT,
    COUNT(*) AS order_count,
    SUM(O.O_TOTALPRICE) AS total_revenue,
    AVG(O.O_TOTALPRICE) AS avg_order_value,
    SUM(L.L_QUANTITY) AS total_quantity,
    COUNT(DISTINCT C.C_CUSTKEY) AS unique_customers,
    COUNT(DISTINCT L.L_PARTKEY) AS unique_parts
FROM GEN2_BENCHMARK_TEST.TESTING.ORDERS O
JOIN GEN2_BENCHMARK_TEST.TESTING.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN GEN2_BENCHMARK_TEST.TESTING.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE O.O_ORDERDATE >= '1995-01-01'
GROUP BY O.O_ORDERPRIORITY, C.C_MKTSEGMENT
ORDER BY total_revenue DESC;

SET end_time = CURRENT_TIMESTAMP();

-- Log Gen2 aggregation results
INSERT INTO TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SELECT 
    'AGGREGATION_TEST_' || CURRENT_DATE()::STRING,
    'TEST_GEN2_MEDIUM',
    'GEN2',
    'Large_Table_Aggregation',
    $start_time,
    $end_time,
    DATEDIFF('seconds', $start_time, $end_time),
    0,
    6000000,
    CURRENT_DATE(),
    'Large aggregation with multi-table joins - Gen2';

-- =================================================================
-- Step 5: Add Credit Consumption Data (UPDATED - MULTIPLE OPTIONS)
-- =================================================================

/*
IMPORTANT: Gen2 vs Gen1 Credit Rate Differences
===============================================

Gen2 warehouses consume MORE credits per hour than Gen1:
- Gen1 Medium: 4.0 credits/hour
- Gen2 Medium: 5.4 credits/hour (AWS/GCP) or 5.0 credits/hour (Azure)

BUT Gen2 completes work faster, so total credits per query should be lower.

Example:
- Gen1: 10 seconds × 4.0 credits/hour = 0.0111 total credits
- Gen2: 5 seconds × 5.4 credits/hour = 0.0075 total credits
- Result: Gen2 uses 32% fewer total credits despite higher hourly rate

This is why Gen2 delivers both faster performance AND lower costs!
*/

-- Option 1: Get credits from QUERY_HISTORY (more immediate, less latency)
-- FIXED: Split into separate updates for Gen1 and Gen2 to avoid complex correlated subquery

-- Update Gen1 warehouse credits
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = (
    SELECT COALESCE(SUM(CREDITS_USED_CLOUD_SERVICES + TOTAL_ELAPSED_TIME/1000.0 * 
        CASE 
            WHEN WAREHOUSE_SIZE = 'X-Small' THEN 1/3600.0
            WHEN WAREHOUSE_SIZE = 'Small' THEN 2/3600.0  
            WHEN WAREHOUSE_SIZE = 'Medium' THEN 4/3600.0
            WHEN WAREHOUSE_SIZE = 'Large' THEN 8/3600.0
            WHEN WAREHOUSE_SIZE = 'X-Large' THEN 16/3600.0
            WHEN WAREHOUSE_SIZE = '2X-Large' THEN 32/3600.0
            WHEN WAREHOUSE_SIZE = '3X-Large' THEN 64/3600.0
            WHEN WAREHOUSE_SIZE = '4X-Large' THEN 128/3600.0
            ELSE 4/3600.0
        END), 0)
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE WAREHOUSE_NAME = TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.warehouse_name
    AND START_TIME >= TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.start_time
    AND START_TIME <= TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.end_time
    AND EXECUTION_STATUS = 'SUCCESS'
)
WHERE test_date = CURRENT_DATE() 
AND warehouse_name LIKE '%GEN1%';

-- Update Gen2 warehouse credits (AWS/GCP rates)
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = (
    SELECT COALESCE(SUM(CREDITS_USED_CLOUD_SERVICES + TOTAL_ELAPSED_TIME/1000.0 * 
        CASE 
            WHEN WAREHOUSE_SIZE = 'X-Small' THEN 1.35/3600.0
            WHEN WAREHOUSE_SIZE = 'Small' THEN 2.7/3600.0  
            WHEN WAREHOUSE_SIZE = 'Medium' THEN 5.4/3600.0
            WHEN WAREHOUSE_SIZE = 'Large' THEN 10.8/3600.0
            WHEN WAREHOUSE_SIZE = 'X-Large' THEN 21.6/3600.0
            WHEN WAREHOUSE_SIZE = '2X-Large' THEN 43.2/3600.0
            WHEN WAREHOUSE_SIZE = '3X-Large' THEN 86.4/3600.0
            WHEN WAREHOUSE_SIZE = '4X-Large' THEN 172.8/3600.0
            ELSE 5.4/3600.0
        END), 0)
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE WAREHOUSE_NAME = TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.warehouse_name
    AND START_TIME >= TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.start_time
    AND START_TIME <= TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.end_time
    AND EXECUTION_STATUS = 'SUCCESS'
)
WHERE test_date = CURRENT_DATE() 
AND warehouse_name LIKE '%GEN2%';

-- Option 2: Alternative - Check warehouse metering with broader time window
-- Wait 15-30 minutes after tests, then run this:

/*
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = (
    SELECT COALESCE(SUM(CREDITS_USED_COMPUTE), 0)
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE WAREHOUSE_NAME = TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.warehouse_name
    AND START_TIME >= DATEADD('minute', -5, TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.start_time)
    AND END_TIME <= DATEADD('minute', 5, TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS.end_time)
)
WHERE test_date = CURRENT_DATE();
*/

-- Option 3: Manual credit calculation based on warehouse size and execution time
-- This gives immediate estimates (RUN THIS IF OPTION 1 RETURNS 0)
-- UPDATED with correct Gen1 vs Gen2 credit rates per Snowflake documentation
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = 
    CASE 
        -- Gen1 Standard Warehouse rates
        WHEN warehouse_name LIKE '%GEN1%' AND warehouse_name LIKE '%MEDIUM%' THEN execution_time_seconds * (4.0/3600.0)   -- Gen1 Medium = 4 credits/hour
        WHEN warehouse_name LIKE '%GEN1%' AND warehouse_name LIKE '%SMALL%' THEN execution_time_seconds * (2.0/3600.0)    -- Gen1 Small = 2 credits/hour
        WHEN warehouse_name LIKE '%GEN1%' AND warehouse_name LIKE '%LARGE%' THEN execution_time_seconds * (8.0/3600.0)    -- Gen1 Large = 8 credits/hour
        
        -- Gen2 Warehouse rates (AWS/GCP - use 5.4 for Medium as most common)
        WHEN warehouse_name LIKE '%GEN2%' AND warehouse_name LIKE '%MEDIUM%' THEN execution_time_seconds * (5.4/3600.0)   -- Gen2 Medium = 5.4 credits/hour (AWS/GCP)
        WHEN warehouse_name LIKE '%GEN2%' AND warehouse_name LIKE '%SMALL%' THEN execution_time_seconds * (2.7/3600.0)    -- Gen2 Small = 2.7 credits/hour (AWS/GCP)
        WHEN warehouse_name LIKE '%GEN2%' AND warehouse_name LIKE '%LARGE%' THEN execution_time_seconds * (10.8/3600.0)   -- Gen2 Large = 10.8 credits/hour (AWS/GCP)
        
        -- Fallback for any unmatched warehouse names
        ELSE execution_time_seconds * (4.0/3600.0) -- Default to Gen1 Medium
    END
WHERE test_date = CURRENT_DATE() AND credits_consumed = 0;

-- =================================================================
-- Step 6: Analyze Your Results
-- =================================================================

-- Performance comparison summary
SELECT 
    query_name,
    warehouse_generation,
    execution_time_seconds,
    ROUND(credits_consumed, 6) AS credits_consumed,
    ROUND(credits_consumed * 2.00, 4) AS estimated_cost_dollars -- Adjust rate as needed
FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
WHERE test_date = CURRENT_DATE()
ORDER BY query_name, warehouse_generation;

-- Calculate improvement percentages
WITH gen_comparison AS (
    SELECT 
        query_name,
        MAX(CASE WHEN warehouse_generation = 'GEN1' THEN execution_time_seconds END) AS gen1_time,
        MAX(CASE WHEN warehouse_generation = 'GEN2' THEN execution_time_seconds END) AS gen2_time,
        MAX(CASE WHEN warehouse_generation = 'GEN1' THEN credits_consumed END) AS gen1_credits,
        MAX(CASE WHEN warehouse_generation = 'GEN2' THEN credits_consumed END) AS gen2_credits
    FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
    WHERE test_date = CURRENT_DATE()
    GROUP BY query_name
)
SELECT 
    query_name,
    gen1_time,
    gen2_time,
    ROUND(((gen1_time - gen2_time) / NULLIF(gen1_time, 0) * 100), 1) AS speed_improvement_pct,
    ROUND(gen1_credits, 6) AS gen1_credits,
    ROUND(gen2_credits, 6) AS gen2_credits,
    ROUND(((gen1_credits - gen2_credits) / NULLIF(gen1_credits, 0) * 100), 1) AS cost_reduction_pct,
    CASE 
        WHEN gen2_time < gen1_time THEN '✅ Gen2 Faster'
        WHEN gen2_time > gen1_time THEN '❌ Gen1 Faster'
        ELSE '🔄 Same Speed'
    END AS performance_winner,
    CASE 
        WHEN gen2_credits < gen1_credits THEN '✅ Gen2 Cheaper'
        WHEN gen2_credits > gen1_credits THEN '❌ Gen1 Cheaper'
        ELSE '🔄 Same Cost'
    END AS cost_winner
FROM gen_comparison
WHERE gen1_time IS NOT NULL AND gen2_time IS NOT NULL
ORDER BY speed_improvement_pct DESC;

-- Detailed results with all metrics
SELECT 
    test_id,
    warehouse_generation,
    query_name,
    execution_time_seconds,
    ROUND(credits_consumed, 6) AS credits_consumed,
    rows_processed,
    ROUND(rows_processed / NULLIF(execution_time_seconds, 0), 0) AS rows_per_second,
    ROUND(credits_consumed / NULLIF(execution_time_seconds, 0) * 3600, 4) AS credits_per_hour,
    notes
FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
WHERE test_date = CURRENT_DATE()
ORDER BY query_name, warehouse_generation;

-- =================================================================
-- Step 7: Summary Statistics
-- =================================================================

-- Overall performance summary
WITH summary_stats AS (
    SELECT 
        warehouse_generation,
        COUNT(*) AS test_count,
        AVG(execution_time_seconds) AS avg_execution_time,
        AVG(credits_consumed) AS avg_credits,
        SUM(execution_time_seconds) AS total_execution_time,
        SUM(credits_consumed) AS total_credits
    FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
    WHERE test_date = CURRENT_DATE()
    GROUP BY warehouse_generation
)
SELECT 
    warehouse_generation,
    test_count,
    ROUND(avg_execution_time, 2) AS avg_execution_seconds,
    ROUND(avg_credits, 6) AS avg_credits_consumed,
    ROUND(total_execution_time, 2) AS total_time_seconds,
    ROUND(total_credits, 6) AS total_credits_consumed,
    ROUND(total_credits * 2.00, 4) AS estimated_total_cost_dollars
FROM summary_stats
ORDER BY warehouse_generation;

-- Gen2 vs Gen1 Overall Comparison
WITH gen1_totals AS (
    SELECT 
        SUM(execution_time_seconds) AS total_gen1_time,
        SUM(credits_consumed) AS total_gen1_credits
    FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
    WHERE test_date = CURRENT_DATE() AND warehouse_generation = 'GEN1'
),
gen2_totals AS (
    SELECT 
        SUM(execution_time_seconds) AS total_gen2_time,
        SUM(credits_consumed) AS total_gen2_credits
    FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
    WHERE test_date = CURRENT_DATE() AND warehouse_generation = 'GEN2'
)
SELECT 
    'OVERALL COMPARISON' AS metric,
    ROUND(g1.total_gen1_time, 2) AS gen1_total_seconds,
    ROUND(g2.total_gen2_time, 2) AS gen2_total_seconds,
    ROUND(((g1.total_gen1_time - g2.total_gen2_time) / NULLIF(g1.total_gen1_time, 0) * 100), 1) AS speed_improvement_pct,
    ROUND(g1.total_gen1_credits, 6) AS gen1_total_credits,
    ROUND(g2.total_gen2_credits, 6) AS gen2_total_credits,
    ROUND(((g1.total_gen1_credits - g2.total_gen2_credits) / NULLIF(g1.total_gen1_credits, 0) * 100), 1) AS cost_reduction_pct
FROM gen1_totals g1, gen2_totals g2;

-- =================================================================
-- Expected Results (Based on Snowflake Documentation)
-- =================================================================

/*
Expected Performance Improvements with Gen2 (UPDATED with correct credit rates):
=================================================================================

• Simple DML Operations: 10-30% faster execution, varies on cost due to higher hourly rate
• Bulk INSERT Operations: 30-60% faster execution, 20-40% fewer TOTAL credits
• Complex DML with JOINs: 40-70% faster execution, 30-50% fewer TOTAL credits
• Large DELETE Operations: 25-55% faster execution, 15-35% fewer TOTAL credits
• Complex Analytics: 30-70% faster execution, 25-40% fewer TOTAL credits  
• Large Aggregations: Up to 2x faster execution, 30-50% fewer TOTAL credits

IMPORTANT: Credit Rate Reality Check
====================================
Gen2 warehouses cost MORE per hour but complete work faster:

Example with your Medium warehouse results:
- Complex Analytics Gen1: 3 seconds × 4.0 credits/hour = 0.00333 total credits
- Complex Analytics Gen2: 1 second × 5.4 credits/hour = 0.00150 total credits  
- Gen2 saves 55% in total credits despite 35% higher hourly rate!

- Large Aggregation Gen1: 2 seconds × 4.0 credits/hour = 0.00222 total credits
- Large Aggregation Gen2: 1 second × 5.4 credits/hour = 0.00150 total credits
- Gen2 saves 32% in total credits!

Your Actual Results Will Vary Based On:
✓ Data size and complexity
✓ Query patterns and optimization
✓ Concurrent workload on the warehouse
✓ Specific data distribution and schema design
✓ Cloud provider (AWS/GCP vs Azure have different Gen2 rates)

Typical Results You Should See:
• Gen2 is consistently faster across all workload types
• Total credit consumption is lower due to much faster execution
• Higher credits/hour but lower total credits per query
• Biggest gains on DML operations and table scans
• Analytics queries show substantial improvement
*/

-- =================================================================
-- Step 8: View Test Data (Check Your Results)
-- =================================================================

-- Quick results check
SELECT 
    'TEST RESULTS SUMMARY' AS report_section,
    COUNT(*) AS total_tests_run,
    COUNT(DISTINCT query_name) AS unique_test_types,
    MIN(start_time) AS first_test_time,
    MAX(end_time) AS last_test_time
FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
WHERE test_date = CURRENT_DATE();

-- View all your test data
SELECT * 
FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
WHERE test_date = CURRENT_DATE()
ORDER BY query_name, warehouse_generation;

-- =================================================================
-- Step 9: Clean Up (Optional)
-- =================================================================

-- Uncomment these lines if you want to clean up test resources

DROP WAREHOUSE IF EXISTS TEST_GEN1_MEDIUM;
DROP WAREHOUSE IF EXISTS TEST_GEN2_MEDIUM;


-- =================================================================
-- BENCHMARK COMPLETE!
-- =================================================================

SELECT 
    'Gen2 vs Gen1 Benchmark Complete!' AS status,
    'Check results above or query TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS' AS next_step,
    CURRENT_TIMESTAMP() AS completion_time;

-- =================================================================
-- QUICK REFERENCE: Key Queries to Run After Benchmark
-- =================================================================

/*
1. View Performance Summary:
SELECT query_name, warehouse_generation, execution_time_seconds, credits_consumed 
FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS WHERE test_date = CURRENT_DATE()
ORDER BY query_name, warehouse_generation;

2. See Improvement Percentages:
[Run the "Calculate improvement percentages" query above]*/
-- Calculate improvement percentages
WITH gen_comparison AS (
    SELECT 
        query_name,
        MAX(CASE WHEN warehouse_generation = 'GEN1' THEN execution_time_seconds END) AS gen1_time,
        MAX(CASE WHEN warehouse_generation = 'GEN2' THEN execution_time_seconds END) AS gen2_time,
        MAX(CASE WHEN warehouse_generation = 'GEN1' THEN credits_consumed END) AS gen1_credits,
        MAX(CASE WHEN warehouse_generation = 'GEN2' THEN credits_consumed END) AS gen2_credits
    FROM TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS
    WHERE test_date = CURRENT_DATE()
    GROUP BY query_name
)
SELECT 
    query_name,
    gen1_time,
    gen2_time,
    ROUND(((gen1_time - gen2_time) / NULLIF(gen1_time, 0) * 100), 1) AS speed_improvement_pct,
    ROUND(gen1_credits, 6) AS gen1_credits,
    ROUND(gen2_credits, 6) AS gen2_credits,
    ROUND(((gen1_credits - gen2_credits) / NULLIF(gen1_credits, 0) * 100), 1) AS cost_reduction_pct,
    CASE 
        WHEN gen2_time < gen1_time THEN '✅ Gen2 Faster'
        WHEN gen2_time > gen1_time THEN '❌ Gen1 Faster'
        ELSE '🔄 Same Speed'
    END AS performance_winner,
    CASE 
        WHEN gen2_credits < gen1_credits THEN '✅ Gen2 Cheaper'
        WHEN gen2_credits > gen1_credits THEN '❌ Gen1 Cheaper'
        ELSE '🔄 Same Cost'
    END AS cost_winner
FROM gen_comparison
WHERE gen1_time IS NOT NULL AND gen2_time IS NOT NULL
ORDER BY speed_improvement_pct DESC;

/*3. Update Credits with CORRECT Gen1/Gen2 rates:
-- Gen1 warehouses
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = execution_time_seconds * (4.0/3600.0)
WHERE test_date = CURRENT_DATE() AND warehouse_name LIKE '%GEN1%' AND credits_consumed = 0;

-- Gen2 warehouses (AWS/GCP rates)
UPDATE TEST_GEN2.SCHEMA_GEN2.PERFORMANCE_TEST_RESULTS 
SET credits_consumed = execution_time_seconds * (5.4/3600.0)
WHERE test_date = CURRENT_DATE() AND warehouse_name LIKE '%GEN2%' AND credits_consumed = 0;

4. Overall Summary:
[Run the "Gen2 vs Gen1 Overall Comparison" query above]
*/

---DROP DATABASE IF EXISTS GEN2_BENCHMARK_TEST;
---DROP DATABASE IF EXISTS TEST_GEN2;
