/*
Author - Tirth Vyas
Created - 12th March, 2026
==========================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================================================
Script Purpose:
	This script defines a stored procedure which load data into the bronze layer/schema from external .CSV files.
	It performs the following:
		1. Truncates the bronze tables
		2. Loads data using BULK INSERT from the source data in .CSV form to the bronze schema

Usage Example after Executing this script:
	EXEC bronze.load_bronze;

-> The init_bronze_tables script is the prerequisite for this script, run this after.

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_batch DATETIME, @end_time_batch DATETIME;
	BEGIN TRY
		SET @start_time_batch = GETDATE()
		PRINT '==================================================================';
		PRINT 'LOADING IN BRONZE LAYER';
		PRINT '==================================================================';

		PRINT '------------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '------------------------------------------------------------------';

		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Loading table: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Loading table: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Loading table: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		PRINT '------------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '------------------------------------------------------------------';
		
		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Loading table: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Loading table: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		SET @start_time = GETDATE()

		PRINT '>> Truncating table: bronze.erp_px_cat_glv2'
		TRUNCATE TABLE bronze.erp_px_cat_glv2;

		PRINT '>> Loading table: bronze.erp_px_cat_glv2'
		BULK INSERT bronze.erp_px_cat_glv2
		FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
		
		
		SET @end_time_batch = GETDATE()
		PRINT '>> BATCH LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------'; 
	END TRY
	BEGIN CATCH
		PRINT '=======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_MESSAGE() AS NVARCHAR)
		PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '=======================================================================';
	END CATCH
END