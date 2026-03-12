USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


DROP TABLE bronze.crm_cust_info;

DROP SCHEMA bronze;
GO
DROP SCHEMA silver;
GO
DROP SCHEMA gold;
GO

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_in INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_materian_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_data DATE
);

EXEC sp_rename 
'bronze.crm_cust_info.cst_materian_status',
'cst_marital_status',
'COLUMN';

EXEC sp_rename 
'bronze.crm_cust_info.cst_in',
'cst_id',
'COLUMN';

EXEC sp_rename 
'bronze.crm_cust_info.cst_create_data',
'cst_create_date',
'COLUMN';

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prod_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_slales INT,
	sls_quantity INT,
	sls_price INT
);

EXEC sp_rename 
'bronze.crm_sales_details.sls_slales',
'sls_sales',
'COLUMN';

EXEC sp_rename 
'bronze.crm_sales_details.sls_prod_key',
'sls_prd_key',
'COLUMN';

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_glv2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_glv2;
CREATE TABLE bronze.erp_px_cat_glv2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

-- NOW LETS LOAD THE DATA IN THE DATABASE

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)

--TEST crm_cust_info table

SELECT COUNT(*)
FROM bronze.crm_cust_info;
---------------------------------------------------------------------------------------------------------
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
SELECT COUNT(*)
FROM bronze.crm_prd_info;

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
SELECT COUNT(*)
FROM bronze.crm_sales_details;

TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
SELECT COUNT(*)
FROM bronze.erp_loc_a101;

TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
SELECT COUNT(*)
FROM bronze.erp_cust_az12;

TRUNCATE TABLE bronze.erp_px_cat_glv2;
BULK INSERT bronze.erp_px_cat_glv2
FROM 'C:\Users\tirth\Documents\Projects\DatawareHouse_1\GIT Source\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
)
SELECT COUNT(*)
FROM bronze.erp_px_cat_glv2;


SELECT TOP 1000 * FROM bronze.crm_cust_info;
SELECT TOP 1000 * FROM bronze.crm_prd_info;
SELECT TOP 1000 * FROM bronze.crm_sales_details;

SELECT TOP 1000 * FROM bronze.erp_loc_a101;
SELECT TOP 1000 * FROM bronze.erp_cust_az12;
SELECT TOP 1000 * FROM bronze.erp_px_cat_glv2;

-- Data cleaning

-- Table: bronze.crm_cust_info
-- Check and remove duplicates in a column
SELECT
cst_id,
COUNT(*) AS test
FROM silver.crm_cust_info
GROUP BY crm_cust_info.cst_id
ORDER BY test DESC;

SELECT
cst_id,
COUNT(*) AS test
FROM silver.crm_cust_info
GROUP BY crm_cust_info.cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT
*
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;

-- Check unwanted spaces

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)


SELECT
cst_id,
TRIM(cst_key) AS cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
TRIM(cst_marital_status) AS cst_marital_status,
TRIM(cst_gndr) AS cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;

-- Data standardization & consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT
cst_id,
TRIM(cst_key) AS cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END AS cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;

-- Now insert into Silver:

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT
cst_id,
TRIM(cst_key) AS cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END AS cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


ALTER TABLE silver.crm_cust_info
DROP COLUMN dwh_create_date;

-- Table: bronze.crm_prd_info
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Split Prd_key
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT id from bronze.erp_px_cat_glv2);


SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN
(SELECT sls_prd_key FROM bronze.crm_sales_details) --WHERE crm_sales_details.sls_prd_key LIKE 'FK-%')

-- Check wanted spaces:

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check if nullls and negative numbers

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


SELECT DISTINCT [prd_line]
FROM [bronze].[crm_prd_info];


SELECT [prd_line]
FROM [bronze].[crm_prd_info]
WHERE [prd_line] IS NULL;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;



SELECT
*
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


SELECT
--prd_id,
--prd_key,
--prd_nm,
--prd_start_dt,
--prd_end_dt,
*,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
ORDER BY prd_id;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST((LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT
*
FROM silver.crm_prd_info


-- Last table in crm = crm_sales_details

-- Check if the foriegn key is inclusive of all the primary keys in another table or vice-versa
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Convert dats saved in format ######## to dates

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_test,
sls_ship_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_test,
sls_due_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_test,
sls_ship_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details


----How to check NULLS
SELECT
*
FROM bronze.crm_sales_details
WHERE NULLIF(sls_order_dt, 0) <= 0

-- Other checks

SELECT
NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE NULLIF(sls_ship_dt, 0) <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20500101
	OR sls_ship_dt < 19000101

-- Order date should always be less than shipping date

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistancy, for sales qunatity and price

SELECT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT
sls_sales,
sls_quantity,
sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales_new,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN ABS(sls_price) / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price_new
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT
*
FROM bronze.crm_sales_details;

-- Full data cleaining query for crm_sales_details

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN ABS(sls_price) / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

-- Cleaning and loading erp_cust_az12

SELECT * FROM silver.crm_cust_info

-- Checking the primary key
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	  END NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Check if any date is out of range

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


SELECT DISTINCT 
	gen,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- table erp_loc_a101

-- Remove - in cid

SELECT
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- Data Standardization & Consistency
-- Manage Consistency in cntry column
SELECT DISTINCT
cntry AS old_cntry,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry