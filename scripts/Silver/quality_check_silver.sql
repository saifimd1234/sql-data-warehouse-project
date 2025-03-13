-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Output: There are duplicate values but no NULL values.