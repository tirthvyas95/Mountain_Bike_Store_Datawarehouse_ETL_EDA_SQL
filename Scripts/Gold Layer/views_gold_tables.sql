/*
Author - Tirth Vyas
Created - 12th March, 2026
=========================================================================================================
DDL Script: Create Gold Views
=========================================================================================================
Script Purpose:
	This script creates views for the Gold Layer in the Data Warehouse.
	The Gold layer is the Final Stage in our Warehouse Architecture where the data is ready to be used
	by report and dashboard developers.
	Here, we have made 3 Views for 3 Tables:
		1. dim_customers: Dimension table with details pertaining to customers
		2. dim_products: Dimension table with details related to products
		3. fact_sales: Fact table with all the sales records
=========================================================================================================
Usage:
First execute the entire script and then you can SELECT FROM these Views like this:

SELECT
*
FROM gold.fact_sales;

*/

-- GOLD view DIM customer

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
(
	SELECT
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, --Surrogate Key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status as marital_status,
		CASE
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
			ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date as create_date
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid
);
GO

-- GOLD VIEW DIM product
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO


CREATE VIEW gold.dim_products AS 
(
	SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, --Surrogate Key
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date,
	pn.prd_end_dt
	FROM silver.crm_prd_info as pn
	LEFT JOIN silver.erp_px_cat_glv2 AS pc
	ON pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL --Filter out all historical data
);
GO

-- GOLD VIEW FACT sales
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE OR ALTER VIEW gold.fact_sales AS
(
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_Key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
	FROM silver.crm_sales_details AS sd
	LEFT JOIN gold.dim_products as pr
	ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers as cu
	ON sd.sls_cust_id = cu.customer_id
);
GO
