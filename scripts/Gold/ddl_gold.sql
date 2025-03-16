SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key
	ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_create_date,
    CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM is the primary source for gender
		ELSE COALESCE(ca.gen, 'n/a')   -- Fallback to ERP data
	END gender,
    ca.bdate,
    la.cntry
FROM 
    silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca 
    ON ci.cst_id = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
    ON ca.cid = la.cid;
