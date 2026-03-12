# Mountain_Bike_Store_Datawarehouse_ETL_EDA_SQL
[high level architechure of the data warehouse]
## Introduction
We play a role as a data engineer who works for a certain company which sells Mountain Bikes and related Accessories. Here we must make a Data Warehouse enabling us to centralize, clean, and store vast amounts of data helping Data Analysts and Business Analysts have access to fresh and cleaned data for making reports and dashboards which in turn will help stakeholders make faster data-driven decisions.

## Workflow:
For this given problem we will create 3 layers of abstraction, we can name them anything for my case I have named them Bronze, Silver and Gold. Where we will load the RAW data into Bronze which will most likely be unclean, with duplicates, with NULLs, with blank spaces, etc. we will store this raw data just in case we need to go back or retrace our steps if we encounter a problem 
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

Here is how the tables look like in each layer:
[Tables in Each layer]
