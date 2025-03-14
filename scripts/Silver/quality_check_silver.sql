-- =============================================================
-- QUALITY CHECK FOR BRONZE LAYER. (Table: bronze.crm_cust_info)
-- =============================================================
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
-- QUALITY CHECK FOR SILVER LAYER. (Table: silver.crm_cust_info)
-- =============================================================
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
-- Expectation: only 3 types of values (n/a, Male, Female)
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- =============================================================
-- QUALITY CHECK FOR BRONZE LAYER. (Table: bronze.crm_prd_info)
-- =============================================================

-- Check for NULLs or Duplicates in Primary Key.
-- Expectation: No Results
SELECT
    prd_id,
    COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces.
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- =============================================================
-- QUALITY CHECK FOR SILVER LAYER. (Table: silver.crm_prd_info)
-- Re-run the above queires by replacing bronze with silver.
-- =============================================================

-- =============================================================
-- QUALITY CHECK FOR BRONZE LAYER. (Table: bronze.crm_sales_details)
-- =============================================================
-- For string columns check for unwanted spaces.
-- Expectation: No Results

-- As we are using prd_key (sls_prd_key) and cst_id (sls_cust_id) to connect the two tables so here we can check the integrity of these columns.

SELECT
    [sls_ord_num],
    [sls_prd_key],
    [sls_cust_id],
    [sls_order_dt],
    [sls_ship_dt],
    [sls_due_dt],
    [sls_sales],
    [sls_quantity],
    [sls_price]
FROM [DataWarehouse].[bronze].[crm_sales_details]
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Similarly, we can check the integrity of the columns in the silver layer.
-- Using NOT IN we can check the sls_prd_key that are not present in prd_key in other table.
-- Since, we are not getting any output that means everything is good and we can go and connect them without any issues.

SELECT
    [sls_ord_num],
    [sls_prd_key],
    [sls_cust_id],
    [sls_order_dt],
    [sls_ship_dt],
    [sls_due_dt],
    [sls_sales],
    [sls_quantity],
    [sls_price]
FROM [DataWarehouse].[bronze].[crm_sales_details]
WHERE sls_prd_key NOT IN (
    SELECT prd_key
    FROM [DataWarehouse].[silver].[crm_prd_info]
);

-- Similarly, we can check the integrity of the customers columns in the silver layer.
-- Using NOT IN we can check the sls_cust_id that are not present in cst_id in other table.
-- Since, we are not getting any output that means everything is good and we can go and connect them without any issues.

SELECT
    [sls_ord_num],
    [sls_prd_key],
    [sls_cust_id],
    [sls_order_dt],
    [sls_ship_dt],
    [sls_due_dt],
    [sls_sales],
    [sls_quantity],
    [sls_price]
FROM [DataWarehouse].[bronze].[crm_sales_details]
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM [DataWarehouse].[silver].[crm_cust_info]
);

-- Check for Invalid Dates
SELECT
    sls_order_dt
FROM bronze.crm_sales_details
WHERE ISDATE(sls_order_dt) = 0;
-- ISDATE() function returns 0 if the input is not a valid date. So, if we get any output then we can say that the date is invalid.

-- Alternatively.
SELECT
    sls_order_dt
FROM 
    bronze.crm_sales_details
WHERE 
    sls_order_dt <= 0 
    OR LEN(CONVERT(VARCHAR(8), sls_order_dt)) != 8 
    OR sls_order_dt IS NULL
    OR sls_order_dt < 19000101
    OR sls_order_dt > 20501231;
-- Since, date column is INT datatype so we can check for negative values as well and the length should be 8 and not NULL.

-- We can also check for the boundary values, that is the date should not be less than the start of sales and should not be greater than the specified date by the experts. (or for example te current date).

-- Order date must always be earlier than the ship date or due date.
-- Check for Invalid Date orders.
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Vallues must not be NULL, zero or negative.
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- RULES:
-- 1. If Sales is negative, zero or null, derive it using Quantity and Price.
-- 2. If Price is zero or null, derive it using Sales and Quantity.
-- 3. If Price is negative, convert it to a positive value.

SELECT
    sls_price AS old_sls_price,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_price != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS derived_sales,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS derived_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;

-- =============================================================
-- QUALITY CHECK FOR SILVER LAYER. (Table: silver.crm_sales_details)
-- Re-run the above queries by replacing bronze with silver.
-- =============================================================

-- =============================================================
-- QUALITY CHECK FOR BRONZE LAYER. (Table: bronze.erp_cust_az12)
-- =============================================================

-- View the bronze.erp_cust_az12 table.
SELECT * FROM bronze.erp_cust_az12;

-- The ERP (cid column) dataset table can be connected with the CRM dataset table with cst_key.
-- View both the tables that you want to connect.
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM silver.crm_cust_info;

-- The bronze.erp_cust_az12 has column cid, it does not have all rows NAS.. some are without them.
SELECT
    cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE cid NOT LIKE '%AWS00011000%';

SELECT * FROM silver.crm_cust_info;

-- We are doing the CASE WHEN transformation because not all rows contain the NAS.. some are without them so they will remain as it is. (ELSE block)
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;

-- Checks for any unmatching cid in cst_key, after the transformation.
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN (
        SELECT cst_key
        FROM silver.crm_cust_info
    );

-- Identify Out-of-Range Dates
SELECT
    DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1900-01-01' -- check for very old customers
OR bdate > '2021-12-31';   -- check for birthdays in future

-- Data Standardization & Consistency
-- Expectations: Low cardinality
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- Data Transformation
SELECT 
    DISTINCT gen,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS standardized_gen
FROM bronze.erp_cust_az12;

-- =============================================================
-- QUALITY CHECK FOR SILVER LAYER. (Table: silver.erp_cust_az12)
-- Re-run the above queries by replacing bronze with silver.
-- =============================================================

-- =============================================================
-- QUALITY CHECK FOR BRONZE LAYER. (Table: bronze.erp_loc_a101)
-- =============================================================

-- View the table.
SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101;

-- The cid of ERP table can be used to connect with CRM customer table using cst_key.
-- View both the tables that you want to connect.
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM silver.crm_cust_info;

-- Use REPLACE command to remove the hyphen (-).
SELECT
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;

-- Check for any non-matching cid in cst_key.
SELECT
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
);

-- Data Standardization & Consistency
SELECT
DISTINCT cntry AS old_cntry,
CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE cntry
END AS standardized_cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Next, we will insert these values in the silver layer tables. The DDL statement is not changed as number of columns is same and datatype is also same.
