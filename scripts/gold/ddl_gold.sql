CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_ID) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	loc.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gnd != 'n/a' THEN ci.cst_gnd 
		ELSE COALESCE(bd.gen, 'n/a')
	END gender,
	bd.bdate AS birthdate,
	ci.cst_create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 bd
ON ci.cst_key = bd.cid
LEFT JOIN silver.erp_loc_a101 loc
ON ci.cst_key = loc.cid
GO


CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.prd_cat_id AS category_id,
	pc.cat AS catergory,
	pc.subcat AS subcategory,
	pc.maintenance,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.prd_cat_id = pc.id
WHERE prd_end_dt IS NULL
GO

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_num,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id


							
