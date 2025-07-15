
--setup some OLAP TABLES FOR USAGE in medallion architecture
--Use Resellers2ndHandStuffOLTP

--
--one-time usage for this statement
--CREATE DATABASE Resellers2ndHandStuffOLAP


Use Resellers2ndHandStuffOLAP


--Sales Table
--join resellers and orders
DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_SALES
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_SALES
(
	id INT UNIQUE,
	reseller_id INT, --may not need this but could be useful later..?
	reseller_budget NVARCHAR(50), 
	reseller_kind_of_business NVARCHAR(50), 
	order_sales_month DATE,
	order_id INT,  --multiple order_id
	order_amount FLOAT
)


--Customers Table - users who have at least one order will be in here
--join resellers, orders, and items_in_order
DROP TABLE IF EXISTS OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
CREATE TABLE OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
(
	id INT UNIQUE,
	reseller_id INT,
	reseller_state NVARCHAR(50),
	reseller_city NVARCHAR(50),
	user_id INT, --from orders table
	order_id INT,
	items_in_order_price FLOAT,
	order_service_fee FLOAT,
)




TRUNCATE TABLE [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
INSERT INTO [Resellers2ndHandStuffOLAP].dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS(id, user_id, price, service_fee)
	SELECT MAX(id) AS id, orders.user_id AS user_id, orders.price AS price, orders.service_fee AS service_fee 
	FROM [Resellers2ndHandStuffOLTP].dbo.RESELLERS_2ND_HAND_STUFF_ORDERS orders 


SELECT COUNT(*) AS olapCustomersCnt FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS
SELECT TOP(1000) * FROM dbo.OLAP_RESELLERS_2ND_HAND_STUFF_CUSTOMERS ORDER by id

