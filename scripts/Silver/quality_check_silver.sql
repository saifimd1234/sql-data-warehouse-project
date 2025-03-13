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
WHERE cst_id IN (SELECT cst_id FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1);

-- We can resolve it by keeping the latest record and deleting the rest.

