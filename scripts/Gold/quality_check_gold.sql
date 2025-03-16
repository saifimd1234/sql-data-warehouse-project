-- Convert the two sources of gender to one. CRM is the primary source.
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

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

-- Structure the fact table, bring the dminesion keys into the fact table
SELECT 
    sd.sls_ord_num,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
    FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id;