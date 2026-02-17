INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gnd,
	cst_create_date) 
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname, --removing unwanted spaces
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_gnd)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status, --standardizing & normalizing
	CASE 
		WHEN UPPER(TRIM(cst_gnd)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gnd)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gnd, --standardizing & normalizing
	cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank 
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
)t  WHERE t.rank = 1 --filtering out duplicates


INSERT INTO silver.crm_prd_info(
	prd_id,
	prd_full_key,
	prd_cat_id,
	prd_key,
	prd_nm,
	prd_cost, 
	prd_line,
	prd_start_dt, 
	prd_end_dt
)
SELECT 
prd_id,
prd_key as prd_full_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --deriving new columns
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost, --handling missing data
CASE UPPER(TRIM(prd_line)) --data normalization
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt  AS prd_start_dt,
CASE 
	WHEN prd_end_dt < prd_start_dt THEN DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC))
	ELSE prd_end_dt
END AS prd_end_dt --data enrichment
FROM bronze.crm_prd_info

