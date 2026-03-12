# Mountain_Bike_Store_Datawarehouse_ETL_EDA_SQL
![high level architechure of the data warehouse](https://github.com/tirthvyas95/Mountain_Bike_Store_Datawarehouse_ETL_EDA_SQL/blob/e427df6c65e2799604a740593df6d4bf403b394e/Docs%20%26%20Screenshots/data_architecture.png)
## Introduction
We play a role as a data engineer who works for a certain company which sells Mountain Bikes and related Accessories. Here we must make a Data Warehouse enabling us to centralize, clean, and store vast amounts of data helping Data Analysts and Business Analysts have access to fresh and cleaned data for making reports and dashboards which in turn will help stakeholders make faster data-driven decisions.

## Dataset
We have dataset in form of .csv files which we will use to populate our warehouse which will serve as a center point for data analysts and business analysts alike for retrieving the most recent and relevent data.

Our Fictional company sells mountain bike and it has two systems namely a enterprise resource management system and a customer relationship management system, these systems generate raw data in form of .csv files. To start we have 6 .csv files 3 from ERP system and 3 from CRM system, following is the metadata of these files:

1. crm.cust_info:
  - cst_id: Unique ID for each record in the table
  - cst_key: Unique Key generated for each customer
  - cst_firstname: First name of the customer
  - cst_lastname: Last name of the customer
  - cst_maritals_status: Marital status of each customer(M: maried and S: single)
  - cst_gndr: Gender of the customer(M: Male and F: Female)
  - cst_create_date: Date on which the customer account was created
2. crm.prd_info:
  - prd_id: Unique ID for each record in the table
  - prd_key: Unique Key generated for each product
  - prd_nm: Name of the product
  - prd_line: Product Line of the product
  - prd_start_dt: The date on which the product was introduced
  - prd_end_dt: The date on which the product was discontinued(IF NULL that means that we are still selling it)
3. sls_ord_num:
  - sls_ord_num: Order number of the purchase
  - sls_prd_key: Product key of the product purchased
  - sls_cust_id: The customerID associated with the order
  - sls_order_id: The date on which the order was made
  - sls_ship_dt: The date on which the order was shipped
  - sls_due_dt: The due date of the order
  - sls_sales: The total sales amount of the order
  - sls_quantity: The quantity of the the product ordered
  - sls_price: The price of each product
4. erp_cust_az12:
  - cid: Unique id assigned to each customer
  - bdate: Birthdate or each customer
  - gen: Gender of each customer
5. erp.loc_a101:
  - cid: Unique id assigned to each customer
  - cntry: Country of residence of the customer
6. erp.px_cat_g1v2:
  - id: Unique ID of each product
  - cat: Category of each product
  - subcat: Subcategory for each product
  - maintenance: Whether the products needs maintenance or not(Yes/No)

This is how to source data is laid out which we are storing in the Bronze layer. We need to do two main things on this data first being that we need clean and transform this data so that the relationships can be made from one table to another which we wil take care of in the seconda layer the Silver Layer and finally we need to present this data in a frendly way which is understandable for the analysts and stakeholders which we will do in the Gold layer.

## Workflow:
For this given problem we will create 3 layers of abstraction, we can name them anything for my case I have named them Bronze, Silver and Gold. Where we will load the RAW data into Bronze which will most likely be unclean, with duplicates, with NULLs, with blank spaces, etc. we will store this raw data just in case we need to go back or retrace our steps if we encounter a problem.

In order to make this warehouse you can start by first running the script: init_bronze_tables.sql which will create the schemas and define the tables in the bronze layer. Then you can run init_silver_table.sql which will define the tables in silver where we will store the cleaned data. Now, run the proc_load_bronze.sql which will load the source data into our database, also make sure the path to the .csv files is correct. Additoinally, you can run proc_load_silver.sql which will clean the data and load it in the silver layer. Once you executed these stored procedures you can run them and rerun them when you get fresh data. Finally we can execute the views in the for the gold layer by running views_gold_tables.sql which will make the views which in turn will act like tables for report and dashboard creators.

If you do all the steps correctly your object explorer should like this:
![Object Explorer](https://github.com/tirthvyas95/Mountain_Bike_Store_Datawarehouse_ETL_EDA_SQL/blob/e427df6c65e2799604a740593df6d4bf403b394e/Docs%20%26%20Screenshots/SS_Object_Explorer.png)

Apart from the gold.report_customer and gold.report_products which we will show in the analytics section after showin you metadata for the tables in the gold layers.

Here is how the tables look like in each layer:
![Data Lineage](https://github.com/tirthvyas95/Mountain_Bike_Store_Datawarehouse_ETL_EDA_SQL/blob/e427df6c65e2799604a740593df6d4bf403b394e/Docs%20%26%20Screenshots/data_flow.png)

As you can see in the final layer/ gold layer we have only 3 table because we have accumulated all the data from these 6 tables into just 3 table, 2 of them being dimension tables and 1 of them fact table with the sales.

Here is the metadata for the tables in the final layer:
1. gold.dim_customers:
  - customer_key: Unique key assigned for each customer
  - customer_id: Unique id for each customer
  - customer_number: Unique number for each customer
  - first_name: First name of each customer
  - last_name: Last name of each customer
  - country: The country of the customer
  - marital_status: The marital status of our customer
  - gender: The gender of the customer
  - birthdate: Birthdate of each customer
  - create_date: The date on which the customer account was created
2. gold.dim_products:
  - product_key: Unique product key for each product
  - product_id: Unique product id for each product
  - product_number: Number for each product
  - product_name: Name of the product
  - category_id: Id of the category in which the product belongs
  - category: Name of the category in which the product belongs
  - subcategory: Name of the subcategory in which the product belongs
  - maintenance: Whether the product needs maintenance or not
  - cost: The cost associated to the product
  - product_line: The product line
  - stat_date: The date on which the product was introduced
  - prd_end_dt: The date on which the product was discontinued(IF NULL we are still selling the product)
3. gold.fact_sales:
  - order_number: Unique order number for each order
  - product_key: Product key of the product ordered
  - customer_key: The customer key of the customer that ordered the product
  - order_date: The date on which the order was made
  - shipping_date: The date on which the order was shipped
  - due_date: The date on which the order is due
  - sales_amount: The total sales amount
  - quantity: The quantity of product ordered
  - price: The unit price of the product

We must remark that all these table are in form of views as we do not want to keep unnecessary copies of the data and also because we have already cleaned and stored all the source data in the silver layer. All this abstraction layers also serves as security so that any unauthorised or untrained employee does not get access to the source data. 


