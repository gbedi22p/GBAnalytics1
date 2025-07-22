--setup some OLAP TABLES FOR USAGE in medallion architecture
--Use Resellers2ndHandStuffOLTP


--one-time usage for this statement
--CREATE DATABASE Resellers2ndHandStuffOLAP


Use Resellers2ndHandStuffOLAP

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

--create the tables for the OLAP analysis
--todo:  consider creating SSIS data pipelines instead

TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES(id, 
                                                                                resellers_id, resellers_budget, resellers_kind_of_business,
																				order_sales_month, order_id, order_sold_amt, order_cost_amt)
	SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS id, 
	       r.id AS resellers_id, r.budget AS resellers_budget, r.kind_of_business AS resellers_budget,
	       DATETRUNC(MONTH, o.created_at) AS order_sales_month, o.id AS order_id, iio.item_price AS order_sold_amt, iio.item_cost AS order_cost_amt
	FROM [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS o
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER iio ON o.id = iio.order_id
	INNER JOIN [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_RESELLERS r ON iio.resellers_id = r.id

SELECT COUNT(*) AS olapSalesCnt FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES
SELECT  * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_SALES ORDER by order_id, resellers_id
																				

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
SELECT  * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS ORDER by id, resellers_id, customer_id, order_id

