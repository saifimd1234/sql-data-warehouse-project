SELECT 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_create_date,
    ci.cst_gndr,
    ca.bdate,
    ca.gen,
    la.cntry
FROM 
    silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca 
    ON ci.cst_id = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
    ON ca.cid = la.cid;
