IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gnd NVARCHAR(15),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	prd_full_key NVARCHAR(50),
	prd_cat_id NVARCHAR(10),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT, 
	prd_line NVARCHAR(50),
	prd_start_dt DATE, 
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
GO



IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE, 
	gen NVARCHAR(15),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
GO 



IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
GO


IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(10),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(3),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
GO

IF OBJECT_ID ('silver.ufnConvertCountryCode', 'U') IS NOT NULL
	DROP FUNCTION silver.ufnConvertCountryCode;
GO

CREATE FUNCTION silver.ufnConvertCountryCode
(
	@CountryCode NVARCHAR(50)
)
RETURNS NVARCHAR(50)
AS
BEGIN
	DECLARE @FullName NVARCHAR(50);

	SET @FullName = CASE UPPER(TRIM(@CountryCode)) 
		WHEN 'DE' THEN 'Germany'
		WHEN 'US' THEN 'United States of America'
		WHEN 'USA' THEN 'United States of America'
		WHEN 'UA' THEN 'Ukraine'
		WHEN 'UK' THEN 'United Kingdom'
		WHEN '' THEN 'n/a'
		ELSE @CountryCode
	END

	RETURN @FullName
END
GO

