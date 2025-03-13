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

-- Data Standaraization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_marital_status,
	CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
	cst_create_date
FROM (
	SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1

-- =============================================================
-- QUALITY CHECK FOR SILVER LAYER.
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
    *
FROM silver.crm_cust_info
WHERE cst_id IN (
    SELECT cst_id
    FROM silver.crm_cust_info 
    GROUP BY cst_id 
    HAVING COUNT(*) > 1
);

-- Check for unwanted spaces.
-- Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;