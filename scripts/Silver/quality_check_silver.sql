-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Output: There are duplicate values but no NULL values. Same cst_id is has multiple cst_create_date.

-- Look at all the duplicate values.
SELECT
    *
FROM bronze.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id 
    FROM bronze.crm_cust_info 
    GROUP BY cst_id 
    HAVING COUNT(*) > 1
);

-- We can resolve it by keeping the latest record and deleting the rest.

SELECT
    *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1 
AND cst_id IN (
    SELECT cst_id 
    FROM bronze.crm_cust_info 
    GROUP BY cst_id 
    HAVING COUNT(*) > 1
);

-- Check for unwanted spaces.
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

