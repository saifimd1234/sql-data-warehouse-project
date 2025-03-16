-- After joining the table, check if any duplicates were introduced
SELECT 
    cst_id, 
    COUNT(*) AS duplicate_count
FROM (
    SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key
	ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    la.cntry,
    ci.cst_marital_status,
    
    CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM is the primary source for gender
		ELSE COALESCE(ca.gen, 'n/a')   -- Fallback to ERP data
	END AS gender,
    ca.bdate,
    ci.cst_create_date 
FROM 
    silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca 
    ON ci.cst_id = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
    ON ca.cid = la.cid
)t
GROUP BY 
    cst_id 
HAVING 
    COUNT(*) > 1;