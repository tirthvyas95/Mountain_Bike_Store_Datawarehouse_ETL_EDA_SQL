This folder contains all the SQL scripts made and used to create the data warehouse.
Contents:
  1. Bronze Layer:
     a) init_bronze_tables.sql
     b) proc_load_bronze.sql
  2. Silver Layer:
     a) init_silver_layer.sql
     b) proc_load_silver.sql
  3. Gold Layer:
     a) views_gold_table.sql

When you execute these you will need to execute them one by one in the order mentioned above as they are designed in such way. Once the schema and tables have been defined you can then reuse the stored procedures and views to integrate, store and extract new data for report creation.
  
