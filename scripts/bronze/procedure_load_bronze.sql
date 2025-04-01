/*
Stored procedure : Load Bronze layer (Source -> Bronze)
-------------------------------------------------------
script purpose:
This stores procedure loads data into 'Bronze' schema from external
CSV files. It performs the following actions:
  - Truncate the bronze table before loading data
  - uses the 'BULK INSERT' command to load data from csv files to bronze tables

  Parameters:
    None.
This stored  procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			 PRINT '=====================';
			 PRINT 'lOADING BRONZE LAYER';
			 PRINT '======================';

			 PRINT '---------------------';
			 PRINT 'lOADING CRM TABLES';
			 PRINT '-----------------------';

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info; --avoids duplicarion of tables, empties table first then data is inserted (full load)
			PRINT '>> INSERTING DATA INTO : bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';

		    TRUNCATE TABLE bronze.crm_prd_info
			PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details
			PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'

			 PRINT '----------------------';
			 PRINT 'lOADING ERP  TABLES';
			 PRINT '-----------------------';

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101
			PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12
			PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'

			SET @start_time = GETDATE();
			PRINT '>> TRUNCATING TABLE:  bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			PRINT '>> INSERTING DATA INTO:  bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Admin\OneDrive\Documents\Data Warehouse Portfolio Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
			  FIRSTROW = 2,
			  FIELDTERMINATOR = ',',
			  TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
			PRINT '----------------------------------------------'
			SET @batch_end_time = GETDATE();
			PRINT '================================'
			PRINT 'Load Bronze Layer is completed'
			PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
			PRINT '================================='
	END TRY
	BEGIN CATCH 
	PRINT '===================='
	PRINT 'ERROR OCCURED WHEN LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '====================='
	END CATCH
END
