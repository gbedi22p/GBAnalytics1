--setup some OLAP TABLES FOR USAGE in medallion architecture
--Use Resellers2ndHandStuffOLTP


--one-time usage for this statement
--CREATE DATABASE Resellers2ndHandStuffOLAP
Use Resellers2ndHandStuffOLAP


DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM
(
	date DATETIME,
	day SMALLINT,
	month SMALLINT,
	year SMALLINT
)

--Sales Table
--join resellers and orders
DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES
(
	id INT UNIQUE,
	resellers_id INT, --may not need this but could be useful later..?
	resellers_budget NVARCHAR(50), 
	resellers_kind_of_business NVARCHAR(50), 
	order_sales_month DATE,
	order_id INT,  --multiple order_id
	order_sold_amt FLOAT,
	order_cost_amt FLOAT
)

--Customers Table - users who have at least one order will be in here
--join resellers, orders, and items_in_order
DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
(
	id INT UNIQUE,
	order_id INT,
	order_service_fee FLOAT,
	item_product_name NVARCHAR(100),
	items_in_order_price FLOAT,
	items_in_order_cost FLOAT,
	customer_id INT, --from orders table
	customer_first_name NVARCHAR(100),
	customer_last_name NVARCHAR(100),
	resellers_id INT,
	resellers_state NVARCHAR(50),
	resellers_city NVARCHAR(50),
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
(
	id INT UNIQUE,
	customer_id INT, --from orders table
	order_date_start DATE,
	order_date_end DATE,
	order_date DATE,
	order_id INT,
	order_sold_amt FLOAT,
	order_cost_amt FLOAT,
	items_in_order_id INT,
	item_id INT,
	resellers_id INT
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION_RESULTS
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION_RESULTS
(
	period INT,
	cohort_size INT,
	cohort_retained INT,
	pct_retained FLOAT
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS_CHURNED_RESULTS
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS_CHURNED_RESULTS
(
	customer_status NVARCHAR(20),
	total_customers INT
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN
(
	exp_name NVARCHAR(50),  --'New Resellers available Jan 2021'
	user_id INT,
	variant_type NVARCHAR(50)  --'control' OR 'variant 1'
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES
(
	user_id INT,
	order_id INT,
	resellers_id INT,
	items_in_order_id INT,
	order_sold_amt FLOAT,
	order_sales_month DATE
)

DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS
(
	exp_name NVARCHAR(50),
	variant_type NVARCHAR(50),
	total_cohorted INT,
	mean FLOAT,
	stddev FLOAT
)


--create the tables for the OLAP analysis
--todo:  consider creating SSIS data pipelines instead

TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES(id, 
                                                                                resellers_id, resellers_budget, 
																				resellers_kind_of_business,
																				order_sales_month, order_id, 
																				order_sold_amt, order_cost_amt)
	SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS id, 
	       r.id AS resellers_id, r.budget AS resellers_budget, r.kind_of_business AS resellers_budget,
	       DATETRUNC(MONTH, o.created_at) AS order_sales_month, o.id AS order_id, iio.item_price AS order_sold_amt, iio.item_cost AS order_cost_amt
	FROM [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS o
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER iio ON o.id = iio.order_id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_RESELLERS r ON iio.resellers_id = r.id

SELECT COUNT(*) AS olapSalesCnt FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
SELECT  * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES ORDER by order_id, resellers_id


SELECT order_sales_month, SUM(order_sold_amt) AS total_sales_for_month, COUNT(order_sold_amt) AS num_orders_for_month
FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
GROUP BY order_sales_month
ORDER BY order_sales_month


--total revenue and number of orders per year
SELECT DATEPART(YEAR, order_sales_month), SUM(order_sold_amt) AS total_sales_for_year, COUNT(order_id) AS cnt_orders_for_year
FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
GROUP BY DATEPART(YEAR, order_sales_month)
ORDER BY DATEPART(YEAR, order_sales_month)
																	

--queries to store from OLTP to OLAP queries
TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS(id, order_id, order_service_fee,
                                                                                    item_product_name, items_in_order_price, items_in_order_cost, 
																					customer_id, customer_first_name, customer_last_name,
																					resellers_id, resellers_state, resellers_city)
	SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS id, o.id AS order_id, o.service_fee AS order_service_fee,
	i.name as item_product_name,
	iio.item_price AS items_in_order_price, iio.item_cost AS items_in_order_cost, o.user_id AS customer_id, 
	u.first_name AS customer_first_name, u.last_name AS customer_last_name,
	r.id AS resellers_id, r.contact_state AS resellers_state, r.contact_city AS resellers_city
	FROM [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS o
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER iio ON o.id = iio.order_id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_RESELLERS r ON iio.resellers_id = r.id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_USERS u ON o.user_id = u.id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS i ON iio.item_id = i.id


SELECT COUNT(*) AS olapCustomersCnt FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
SELECT * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS ORDER by id, resellers_id, customer_id, order_id


--OLAP queries to be used for retention calculations
TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION(id, customer_id, 
                                                                                          order_date_start, order_date_end, order_date,
																						  order_id, order_sold_amt, order_cost_amt,
																						  items_in_order_id, item_id, resellers_id)
	SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS id,
	       o.user_id AS customer_id, 
		   FIRST_VALUE(o.created_at) OVER (PARTITION BY o.user_id ORDER BY o.created_at ASC) AS first_order_date,
		   --NOTE:  to get last value, its intuitive to have to use bounded preceding and following to get the last final row
		   LAST_VALUE(o.created_at) OVER (PARTITION BY o.user_id ORDER BY o.created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order_date,
		   o.created_at AS order_date,
		   o.id AS order_id,
		   iio.item_price AS order_sold_amt,
		   iio.item_cost AS order_cost_amt,
		   o.items_in_order_id AS items_in_order_id,
		   iio.item_id AS item_id,
		   iio.resellers_id AS resellers_id
	FROM [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS o
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER iio ON o.id = iio.order_id
	ORDER BY customer_id, first_order_date

SELECT TOP(1000) * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
ORDER BY customer_id, order_date_start, order_date_end


--learning how to use first_value and last_value
--SELECT value, first_value(value) OVER (Order By value) AS first_value, 
--       last_value(value) OVER (Order By value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value
--FROM GENERATE_SERIES(2, 10, 1)

SELECT COUNT(customer_id) AS cntOrderPerCustomerId, customer_id 
FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
GROUP BY customer_id
ORDER BY COUNT(customer_id) DESC

--todo:
--decide on cohort groups:  since these are irregular purchases over a several period vs a SAAS product 
--                          we PROBABLY DONT need to see absence or presence of data for every month being track
--                          will we just group by the year_start or year_start, then top 5 states for example
TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM(date, day, month, year)
	SELECT date AS date, 
		   DATEPART(DAY, subq.date) AS day,
	       DATEPART(MONTH, subq.date) AS month, 
	       DATEPART(YEAR, subq.date) AS year
	FROM
	(
		SELECT DATEADD(DAY, value, '01/01/2016') AS date
		FROM generate_series(0, 365 * 7, 1)
	) subq


--test queries	
SELECT COUNT(*) 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM

SELECT * 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_DATE_DIM
WHERE year = 2020 AND month > 4 AND month < 10
ORDER BY date


--basic retention curve
SELECT subq.customer_id, subq.first_order_date,
       subq2.order_date_start, subq2.order_date_end,
	   DATEDIFF(DAY, subq.first_order_date, subq2.order_date_end) AS period
FROM
(
	SELECT customer_id, MIN(order_date_start) AS first_order_date
	FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
	GROUP BY customer_id
) subq
JOIN [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION subq2 ON subq2.customer_id = subq.customer_id
ORDER BY period



--more complete retention curve, with assumption that there is a date_end for every customer
--if not we would have to ensure no missing data somewhere
--todo: consider breaking up cohorts (if dataset warrants it) by either state OR M/F OR some other user demographic..
--todo:  place this code into a table for Power BI
TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION_RESULTS
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION_RESULTS(period, cohort_size, cohort_retained, pct_retained)
	SELECT outerq2.period,
       first_value(outerq2.cohort_retained) OVER (ORDER BY period) AS cohort_size,
	   outerq2.cohort_retained,
	   outerq2.cohort_retained * 1.0 / 
	    first_value(cohort_retained) OVER (ORDER BY period) AS pct_retained
	FROM
	(
		SELECT outerq.period, 
			   COUNT(DISTINCT outerq.customer_id) AS cohort_retained
		FROM
		(
			SELECT subq.customer_id, subq.first_order_date,
				   subq2.order_date_start, subq2.order_date_end,
				   COALESCE(DATEDIFF(YEAR, subq.first_order_date, subq2.order_date_end), 0) AS period
			FROM
			(
				SELECT customer_id, min(order_date_start) AS first_order_date
				FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
				GROUP BY customer_id
			) subq
			JOIN [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION subq2 ON subq2.customer_id = subq.customer_id
			--ORDER BY period, subq.first_order_date, subq.customer_id
		) outerq
		GROUP BY outerq.period
		--HAVING period > 0
		--ORDER BY period
	) outerq2


SELECT * 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION_RESULTS




--distribution of RESELLERS_2ND_HAND_STUFF_ORDERS by customer
--figure out the distribution of customers by the total counts of orders made
--todo:  figure out when to use Power M in PowerBI with some direct query or should we store in tables that are imported
SELECT outerq.orders_by_cust, 
       COUNT(DISTINCT outerq.customer_id) AS uniq_cust_cnt
FROM
(
	SELECT u.id AS customer_id, COUNT(o.id) AS orders_by_cust, SUM(o.price) AS total_price_by_cust
	FROM [Resellers2ndHandSTuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS o
	LEFT JOIN [Resellers2ndHandSTuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_USERS u ON u.id = o.user_id 
	GROUP BY u.id
	--ORDER BY orders_by_cust
	--DESC
) outerq
GROUP BY outerq.orders_by_cust
ORDER BY uniq_cust_cnt


TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES
--variant A/B testing:  pick key dates to indicate user interface or product availability times
--insert records from Dec 2020
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES(user_id, order_id, resellers_id,
	                                                                                                    items_in_order_id, order_sold_amt, order_sales_month)
	SELECT subq2.customer_id AS user_id, subq.order_id AS order_id, 
	       subq.resellers_id AS resellers_id, subq3.id AS items_in_order_id, 
		   subq3.item_price AS order_sold_amt, subq.order_sales_month AS order_sales_month
	FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES subq
	INNER JOIN [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS subq2 ON subq.order_id = subq2.order_id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER subq3 ON subq.order_id = subq3.order_id
	WHERE DATEPART(YEAR, subq.order_sales_month) = 2020 AND DATEPART(MONTH, subq.order_sales_month) = 12

--insert records from Jan 2021
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES(user_id, order_id, resellers_id,
	                                                                                                    items_in_order_id, order_sold_amt, order_sales_month)
	SELECT subq2.customer_id AS user_id, subq.order_id AS order_id, 
	       subq.resellers_id AS resellers_id, subq3.id AS items_in_order_id, 
		   subq3.item_price AS order_sold_amt, subq.order_sales_month AS order_sales_month
	FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES subq
	INNER JOIN [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS subq2 ON subq.order_id = subq2.order_id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER subq3 ON subq.order_id = subq3.order_id
	WHERE DATEPART(YEAR, subq.order_sales_month) = 2021 AND DATEPART(MONTH, subq.order_sales_month) = 1

SELECT COUNT(*) FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES
SELECT TOP(1000) * FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES

TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN 
--using data from NEW_RESELLERS_AVAILABLE experiment released Jan 2021
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN(exp_name, user_id, variant_type)
	SELECT 'Jan 2021 New Resellers available', subq.user_id, 'Control' 
	FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES subq
	WHERE DATEPART(YEAR, subq.order_sales_month) = 2020 AND DATEPART(MONTH, subq.order_sales_month) = 12 

INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN(exp_name, user_id, variant_type)
	SELECT 'Jan 2021 New Resellers available', subq.user_id, 'Variant 1'
	FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES subq
	WHERE DATEPART(YEAR, subq.order_sales_month) = 2021 AND DATEPART(MONTH, subq.order_sales_month) = 1 

SELECT COUNT(*) 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN
SELECT TOP(1000) * 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN


--todo:  store final records for PowerBI report
TRUNCATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS
INSERT INTO OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS(exp_name, variant_type, total_cohorted, mean, stddev)
	SELECT outerq.exp_name, outerq.variant_type,
	       COUNT(outerq.user_id) AS total_cohorted,
		   AVG(outerq.amount) AS mean,
		   STDEV(outerq.amount) AS stddev
	FROM
	(
		SELECT subq2.exp_name, subq2.variant_type, subq2.user_id,
			   SUM(COALESCE(subq3.order_sold_amt, 0)) AS amount
		FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_EXP_ASSIGN subq2
		INNER JOIN [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES subq3 ON subq2.user_id = subq3.user_id
		WHERE subq2.exp_name = 'Jan 2021 New Resellers available'
		GROUP BY subq2.exp_name, subq2.variant_type, subq2.user_id
	) outerq
	GROUP BY outerq.exp_name, outerq.variant_type

SELECT COUNT(*)
FROM OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS

SELECT TOP(1000) *
FROM OLAP_RESELLERS_2ND_HAND_STUFF_SALES_NEW_RESELLERS_PURCHASES_EXP_RESULTS


--todo:  funnel analysis:  step1 and step2 tables
--step1 table could be based on 'active' flag in user table being false..or

--todo:  consider a DAU, WAU, MAU metric as well perhaps which could also be based on a last_login or active field


--test queries to look at data for a specific customer_id
SELECT innerq.customer_id, 
	   MIN(innerq.order_date) AS order_date_start_min,
       MAX(innerq.order_date) AS order_date_start_max
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION innerq
WHERE customer_id = 156590
GROUP BY innerq.customer_id  
ORDER BY innerq.customer_id, order_date_start_max


SELECT *
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION innerq
WHERE customer_id = 156590


SELECT innerq.customer_id, 
       MIN(innerq.order_date) AS order_date_min,
	   MAX(innerq.order_date) AS order_date_max,
	   DATEDIFF(DAY, MIN(innerq.order_date), MAX(innerq.order_date)) AS total_days_a_customer
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION innerq
GROUP BY innerq.customer_id
ORDER BY total_days_a_customer
ASC


--simplified and final version for calculating churn/lapse testing
--modified from example in O'Reily book since that was written using postgres which doesnt have DATEDIFF function
TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS_CHURNED_RESULTS
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS_CHURNED_RESULTS(customer_status, total_customers)
	SELECT
	   outerq.customer_status,
	   SUM(outerq.customers_for_this_months) AS total_customers
	FROM
	(
		SELECT 
		case when subq.months_since_last <= 23 then 'Current'
			 when subq.months_since_last <= 48 then 'Lapsed'
			 else 'Churned'
			 end AS customer_status,
			 COUNT(*) AS customers_for_this_months
		FROM
		(
			SELECT customer_id,
  					MAX(order_date_start) AS max_order_date,
					DATEDIFF(MONTH, MAX(order_date_start), '08/31/2025') AS months_since_last
			FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES_RETENTION
			GROUP BY customer_id
			--ORDER BY months_since_last ASC
			--ORDER BY max_order_date DESC
		) subq
		GROUP BY subq.months_since_last
	) outerq
	GROUP BY customer_status


SELECT * 
FROM [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS_CHURNED_RESULTS



