-- ADVANCED ANALYTICS

-- Change over time, Time analysis

SELECT
order_date,
SUM(sales_amount) as total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date


-- Change over years
SELECT
YEAR(order_date) as order_year,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- Change over month
SELECT
MONTH(order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)

--Change over year and month

SELECT
YEAR(order_date) as order_year,
MONTH(order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

SELECT
DATETRUNC(month, order_date) as order_date,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);

SELECT
FORMAT(order_date, 'yyyy-MMM') as order_date,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM');


-- CUMULATIVE ANALYSIS

-- Aggregating the data progressively over time which helps us understand whther our busness is growing or declining
	--eg: running total sales by year
	--moving average of sales by month

-- Calculate the total sales per month
-- and the running total of sales over time 

-- OVER MONTH
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales -- FRAME: UNBOUNDED PRECEDING AND CURRENT
FROM
(
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
)t
ORDER BY DATETRUNC(month, order_date)

-- OVER YEAR
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales -- FRAME: UNBOUNDED PRECEDING AND CURRENT
FROM
(
SELECT
DATETRUNC(year, order_date) AS order_date,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(year, order_date)
)t
ORDER BY DATETRUNC(year, order_date)


-- Calculate the total sales per month
-- and the running total of sales over time
-- and also the average sales per month and running average sales over time

-- OVER MONTH
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales, -- FRAME: UNBOUNDED PRECEDING AND CURRENT
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
)t
ORDER BY DATETRUNC(month, order_date);

-- OVER YEAR
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales, -- FRAME: UNBOUNDED PRECEDING AND CURRENT
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT
DATETRUNC(year, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(year, order_date)
)t
ORDER BY DATETRUNC(year, order_date);


-- PERFORMANCE ANALYSIS
-- Compare the current value to the target value

/*Analyze the yearly performace of products by comparing their sales to both
the average sales performance of the product and the previus year's sales*/

WITH yearly_product_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY
YEAR(f.order_date),
p.product_name
)

SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales_product,
(current_sales - (AVG(current_sales) OVER (PARTITION BY product_name))) AS diff_avg,
CASE
	WHEN (current_sales - (AVG(current_sales) OVER (PARTITION BY product_name))) > 0 THEN 'Above Avg'
	WHEN (current_sales - (AVG(current_sales) OVER (PARTITION BY product_name))) < 0 THEN 'Below Avg'
END AS avg_change,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
(current_sales - (LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year))) AS diff_py,
CASE
	WHEN (current_sales - (LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year))) > 0 THEN 'Increase'
	WHEN (current_sales - (LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year))) < 0 THEN 'Decrease'
	ELSE 'No Change'
END AS avg_change_py
FROM yearly_product_sales
ORDER BY product_name, order_year


-- PART TO WHOLE ANALYSIS
-- i.e((Measure/Total Measure) * 100 and group by a dimension)

-- Which categories contribute the most to overall sales?

WITH category_sales AS (
	SELECT
		category,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales AS f
	LEFT JOIN gold.dim_products AS p
	ON f.product_key = p.product_key
	GROUP BY category
)

SELECT
	category,
	total_sales,
	SUM(total_sales) OVER() AS overall_sales,
	ROUND((CAST(total_sales AS FLOAT)/(SUM(total_sales) OVER()) * 100), 2) AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC

-- Which categories contribute the most to overall orders?

WITH category_sales AS (
	SELECT
		category,
		COUNT(DISTINCT order_number) AS total_orders
	FROM gold.fact_sales AS f
	LEFT JOIN gold.dim_products AS p
	ON f.product_key = p.product_key
	GROUP BY category
)

SELECT
	category,
	total_orders,
	SUM(total_orders) OVER() AS overall_total_orders,
	ROUND((CAST(total_orders AS FLOAT)/(SUM(total_orders) OVER()) * 100), 2) AS percentage_of_total
FROM category_sales
ORDER BY total_orders DESC


-- DATA SEGMENTATION

-- Group the data based on a specific range. Helps understand the correlation between two measures
-- Measure BY Measure
-- eg: total products BY sales range
-- eg: total customers BY age

-- Segment products into cost ranges and count how many products fall into each segment

WITH product_segments AS (
SELECT
product_key,
product_name,
cost,
CASE
	WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
END AS cost_range
FROM gold.dim_products
)
SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC


/*
Group customers into three segments based on their spending behaviour:
	- VIP: Customers with at least 12 months of history and spending more than 5000.
	- Regular: Customers with at least 12 months of history but spending 5000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS (
SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
),
customer_segmenting AS (
SELECT
customer_key,
total_spending,
lifespan,
CASE 
	WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
	ELSE 'New'
END AS customer_segment
FROM customer_spending
)

SELECT
COUNT(customer_key) AS total_customer,
customer_segment
FROM customer_segmenting
GROUP BY customer_segment
ORDER BY total_customer DESC