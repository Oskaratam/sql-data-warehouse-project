CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY 
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		SET @batch_start_time = GETDATE()
		SET @start_time = GETDATE()
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Data (after truncate): bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'


		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data (after truncate): bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'


		
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data (after truncate): bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'

	
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Data (after truncate): bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Data (after truncate): bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data (after truncate): bronze.px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\REPOS\DataProjects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------'
		SET @batch_end_time = GETDATE();

		PRINT '====================================================================================================';
		PRINT 'LOADING OF BRONZE LAYER IS COMPLETED';
		PRINT 'Whole Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR);
		PRINT '====================================================================================================';
	END TRY

	BEGIN CATCH
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END

EXEC bronze.load_bronze
