Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF

--DOP caused an issue earlier so this is way to check te DOP at the database level
--however, from my earlier test, using this did not prevent the DOP from going into parallel way
--ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0
--SELECT [value] FROM sys.database_scoped_configurations WHERE [name] = 'MAXDOP';
--ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1
--SELECT [value] FROM sys.database_scoped_configurations WHERE [name] = 'MAXDOP';
	
CREATE OR ALTER PROCEDURE dbo.GENERATE_TABLES_FOR_OLTP
	@MAX_STAGE_1_ROWS_CNT INT,
	@MAX_STAGE_2_ROWS_CNT INT
WITH EXECUTE AS OWNER
AS
BEGIN
	--how to solve the issue where reusing temp_rowset_tables with matching ids will not return
	--the same values over and over, one solution is a simple offset for each new table
	--another solution is a variable offset defined from a table or simply from separate variables
	--a 3rd solution might be to use a mod function based on something so that it could be
	--to do the variable offset might need to fill up a temp table to act as array or use split_string
	--SPLIT_STRING('1,7,9',',')
	--OR temp table with id as first column, val as a second column
	--SELECT TOP(1) * FROM STRING_SPLIT('1,7,9', ',') ORDER BY NEWID()
	--SELECT TOP(1000) id FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_COUPONS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ORDERS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_RESELLERS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_TAXRATES
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_TOKENS
	TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_USERS

	DECLARE @MaxBatchRowSize INT = @MAX_STAGE_1_ROWS_CNT
	DECLARE @BatchRowSize INT = 0
	DECLARE @col1IdOffset INT = 0
	DECLARE @col2IdOffset INT = 0
	DECLARE @col3IdOffset INT = 0
	DECLARE @col4IdOffset INT = 0
	DECLARE @col5IdOffset INT = 0
	DECLARE @col6IdOffset INT = 0
	DECLARE @col7IdOffset INT = 0
	DECLARE @col8IdOffset INT = 0
	DECLARE @col9IdOffset INT = 0
	DECLARE @col10IdOffset INT = 0
	DECLARE @col11IdOffset INT = 0
	DECLARE @col12IdOffset INT = 0
	DECLARE @col13IdOffset INT = 0
	DECLARE @col14IdOffset INT = 0
	DECLARE @col15IdOffset INT = 0
	DECLARE @col16IdOffset INT = 0
	DECLARE @col17IdOffset INT = 0
	DECLARE @col18IdOffset INT = 0
	DECLARE @col19IdOffset INT = 0
	DECLARE @col20IdOffset INT = 0
	DECLARE @col21IdOffset INT = 0
	DECLARE @col22IdOffset INT = 0
	DECLARE @col23IdOffset INT = 0
	DECLARE @col24IdOffset INT = 0

	--dim tables first
	SET NOCOUNT OFF
	DECLARE @TotalLoops INT = 100
	DECLARE @TotalLoopsIdx INT = 0
	--create a temp id offset table so that when generating the final records, our JOINs ids will be different for each loop
	DECLARE @tempIdOffsetTable TABLE(id INT)
	INSERT INTO @tempIdOffsetTable(id)
		SELECT val FROM ##TEMP_SINGLE_INT_TABLE
	SELECT * FROM @tempIdOffsetTable

	--handle dims and simple transaction table creation first
	--1. create the taxes table
	SET @BatchRowSize = 2500  --max 500 cities
	--SELECT TOP(10) * FROM ##TEMP_ROWSET_CITIES
	--SELECT TOP(10) * FROM ##TEMP_ROWSET_SINGLE_INTS
	INSERT INTO RESELLERS_2ND_HAND_STUFF_TAXRATES(id, city, rates)
		SELECT TOP(@BatchRowSize) col1.id, col1.city, col2.val
		FROM ##TEMP_ROWSET_CITIES col1
		JOIN ##TEMP_ROWSET_SINGLE_INTS col2 ON col1.id = col2.id
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_TAXRATES
	SELECT COUNT(*) AS taxratesCnt FROM RESELLERS_2ND_HAND_STUFF_TAXRATES

	--2.  create coupons table (does not depend on any other dim or fact table)
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	SET @BatchRowSize = @MaxBatchRowSize / 500
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_COUPONS(id, created_at, days_to_live, 
		                                             discount_percent, discount_price, minimum_order, name, updated_at, usage_limit)
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize AS id, col2.val AS created_at,
			                          col3.val AS days_to_live, col4.val AS discount_percent, col5.val AS discount_price,  
									  col6.val AS minimum_order, col7.val AS name, col8.val AS updated_at, col9.val AS usage_limit
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_DATETIMES col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col4 ON col1.id = col4.id + @col3IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col5 ON col1.id = col5.id + @col4IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col6 ON col1.id = col6.id + @col5IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col7 ON col1.id = col7.id + @col6IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col8 ON col1.id = col8.id + @col7IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col9 ON col1.id = col9.id + @col8IdOffset
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END	
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_COUPONS -- WHERE id>500000 order by id
	SELECT COUNT(*) AS couponsCnt FROM RESELLERS_2ND_HAND_STUFF_COUPONS 

	--3. create the users table (also doesnt depend on any dim or fact table)
	SET @BatchRowSize = @MaxBatchRowSize / 1.5
	SET @TotalLoops = 1   --todo:  think i will not do the loops because of DOP, 
	                      --so instead will have to either do the loops method from step2 OR just do all ROWSET size at one time
	SET @TotalLoopsIdx = 0
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col10IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col11IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col12IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col13IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col14IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col15IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col16IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col17IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col19IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col20IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col21IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col22IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col23IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col24IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SELECT COUNT(*) AS cntAllUsers FROM RESELLERS_2ND_HAND_STUFF_USERS
		SELECT DISTINCT COUNT(*) AS distinctCntAllUsers FROM RESELLERS_2ND_HAND_STUFF_USERS
		SELECT COUNT(*) AS cntAllUniqueInts FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS
		SELECT DISTINCT COUNT(*) AS distinctCntAllUniqueInts FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS		
		INSERT INTO RESELLERS_2ND_HAND_STUFF_USERS(id, active, braintree_customer_id, business, business_role, contact, created_at,
		                                           cur_location_address, cur_location_location, dev, email, first_name, hashed_password,
												   image_path, is_active, is_compliant, last_name, pass_code, pass_code_expires, 
												   phone_number, reset_password_link, role, salt, updated_at)
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize AS id, col2.val AS active, 
			                          CONCAT(CAST(col3p1.val AS NVARCHAR), col3p2.val) AS braintree_customer_id, 
			                          col4.val AS business, col5.val AS business_role, col6.val AS contact, col7.val AS created_at,
			                          col8.val AS cur_location_address, col9.val AS cur_location_location, col10.val AS dev, col11.email AS email,
									  col12.val AS first_name, CONCAT(CAST(col13p1.val AS NVARCHAR), col13p2.val) AS hashed_password, 
									  col14.val AS image_path, col15.val AS is_active, col16.val AS is_compliant, 
									  col17.val AS last_name, col18.val AS pass_code, DATEADD(MONTH, 7, col19.val) AS pass_code_expires,
									  col20.phone_no AS phone_number, col21.url AS reset_password_link, col22.val AS role, col23.val AS salt, col24.val AS updated_at
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			 INNER JOIN ##TEMP_ROWSET_BOOLS col2 ON col1.id = col2.id + @col1IdOffset
			 INNER JOIN ##TEMP_ROWSET_INTS_4_DIGITS col3p1 ON col1.id = col3p1.id + @col2IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col3p2 ON col1.id = col3p2.id + @col2IdOffset
			 INNER JOIN ##TEMP_ROWSET_SINGLE_INTS col4 ON col1.id = col4.id + @col3IdOffset
			 INNER JOIN ##TEMP_ROWSET_SINGLE_INTS col5 ON col1.id = col5.id + @col4IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col6 ON col1.id = col6.id + @col5IdOffset
			 INNER JOIN ##TEMP_ROWSET_DATETIMES col7 ON col1.id = col7.id + @col6IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col8 ON col1.id = col8.id + @col7IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col9 ON col1.id = col9.id + @col8IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col10 ON col1.id = col10.id + @col9IdOffset
			 INNER JOIN ##TEMP_ROWSET_EMAILS col11 ON col1.id = col11.id + @col10IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col12 ON col1.id = col12.id + @col11IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col13p1 ON col1.id = col13p1.id + @col12IdOffset
			 INNER JOIN ##TEMP_ROWSET_INTS_4_DIGITS col13p2 ON col1.id = col13p2.id + @col13IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col14 ON col1.id = col14.id + @col14IdOffset
			 INNER JOIN ##TEMP_ROWSET_BOOLS col15 ON col1.id = col15.id + @col15IdOffset
			 INNER JOIN ##TEMP_ROWSET_BOOLS col16 ON col1.id = col16.id + @col16IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col17 ON col1.id = col17.id + @col17IdOffset
			 INNER JOIN ##TEMP_ROWSET_INTS_4_DIGITS col18 ON col1.id = col18.id + @col18IdOffset
			 INNER JOIN ##TEMP_ROWSET_DATETIMES col19 ON col1.id = col19.id + @col6IdOffset
			 INNER JOIN ##TEMP_ROWSET_PHONE_NOS col20 ON col1.id = col20.id + @col19IdOffset
			 INNER JOIN ##TEMP_ROWSET_URLS col21 ON col1.id = col21.id + @col20IdOffset
			 INNER JOIN ##TEMP_ROWSET_TEMPWORDS col22 ON col1.id = col22.id + @col21IdOffset
			 INNER JOIN ##TEMP_ROWSET_SINGLE_INTS col23 ON col1.id = col23.id + @col22IdOffset
			 INNER JOIN ##TEMP_ROWSET_DATETIMES col24 ON col1.id = col24.id + @col23IdOffset
			ORDER BY id
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	SELECT COUNT(*) AS cntAllUsers FROM RESELLERS_2ND_HAND_STUFF_USERS
	SELECT DISTINCT COUNT(*) AS distinctCntAllUsers FROM RESELLERS_2ND_HAND_STUFF_USERS	
	
	--4. create the table for the resellers (again doesnt depend on another fact or dim table)
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	SET @BatchRowSize = @MaxBatchRowSize / 100
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col10IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col11IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col12IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col13IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col14IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col15IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col16IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col17IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col19IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col20IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col21IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_RESELLERS(id, budget, kind_of_business, contact_address1, contact_city, contact_place, contact_state,
		                                               contact_timezone, contact_zip, created_at, description, store_type,
													   is_available, is_covid_complaint, is_open, last_crawled, name,
													   operating_hrs, string_id, time_slot_data, updated_at, views, yelp_id)
													   --todo:  type up rest of this stuff here
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize AS id, col2a.val AS budget, col2b.val as kind_of_business, col3.val AS contact_address1, col4.city AS contact_city,
			                          col5.val AS contact_place, col6.state_val AS contact_state, col7.val AS contact_timezone, col8.zip_code AS contact_zip, 
									  col9.val AS created_at, col10.val AS description, col11.val AS store_type, col12.val AS is_available, 
									  col13.val AS is_covid_complaint, col14.val AS is_open, col15.val AS last_crawled, col16.val AS name, col17.val AS operating_hrs,
									  col18.val AS string_id, col19.val AS time_slot_data, col20.val AS updated_at, col21.val AS views, col22.val AS yelp_id
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_BUDGET_CATGS col2a ON col1.id = col2a.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS col2b ON col1.id = col2b.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_CITIES col4 ON col1.id = col4.id + @col3IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col5 ON col1.id = col5.id + @col4IdOffset
			JOIN ##TEMP_ROWSET_STATES col6 ON col1.id = col6.id + @col5IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col7 ON col1.id = col7.id + @col6IdOffset
			JOIN ##TEMP_ROWSET_ZIPS col8 ON col1.id = col8.id + @col7IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col9 ON col1.id = col9.id + @col8IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col10 ON col1.id = col10.id + @col9IdOffset
			JOIN ##TEMP_ROWSET_SINGLE_INTS col11 ON col1.id = col11.id + @col10IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col12 ON col1.id = col12.id + @col11IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col13 ON col1.id = col13.id + @col12IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col14 ON col1.id = col14.id + @col13IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col15 ON col1.id = col15.id + @col14IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col16 ON col1.id = col16.id + @col15IdOffset
			JOIN ##TEMP_ROWSET_SINGLE_INTS col17 ON col1.id = col17.id + @col16IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col18 ON col1.id = col18.id + @col17IdOffset
			JOIN ##TEMP_ROWSET_SINGLE_INTS col19 ON col1.id = col19.id + @col18IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col20 ON col1.id = col20.id + @col19IdOffset
			JOIN ##TEMP_ROWSET_INTS_3_DIGITS col21 ON col1.id = col21.id + @col20IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col22 ON col1.id = col22.id + @col21IdOffset
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	SELECT TOP(1000) * FROM RESELLERS_2ND_HAND_STUFF_RESELLERS  -- WHERE id>500000 order by id
	SELECT COUNT(*) AS resellersCnt FROM RESELLERS_2ND_HAND_STUFF_RESELLERS 

	--access the resellers ids from RESELLERS_2ND_HAND_STUFF_RESELLERS
	--have between 8 and 12 sections for every resellers
	DECLARE @FKItemSections_TotalRowCnt INT = 0
	DECLARE @MinPctCnt FLOAT = 8.0
	DECLARE @MaxPctCnt FLOAT = 12.0
	DECLARE @RandPct FLOAT = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @FKItemSections_TotalRowCnt = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_RESELLERS) 
	--SELECT @RandPct, @FKItemSections_TotalRowCnt
	DECLARE @tempFKItemSections_ResellersIdTable TABLE (id INT, resellers_id INT)
	DECLARE @FKItemSections_ResellersIdTable TABLE (id INT, resellers_id INT)
	DELETE FROM @tempFKItemSections_ResellersIdTable
	INSERT INTO @tempFKItemSections_ResellersIdTable(id, resellers_id)
		SELECT 0, id AS resellers_id FROM RESELLERS_2ND_HAND_STUFF_RESELLERS ORDER BY NEWID()
	SELECT @FKItemSections_TotalRowCnt AS FKItemSections_TotalRowCnt, 
	       (SELECT COUNT(*) + 1 FROM @tempFKItemSections_ResellersIdTable) AS resellersIdTableCnt
	SELECT COUNT(*) AS tempFKItemSectionsResellersIdTableCnt FROM @tempFKItemSections_ResellersIdTable
	SELECT TOP(1000) * FROM @tempFKItemSections_ResellersIdTable
	
	DELETE FROM @FKItemSections_ResellersIdTable
	INSERT INTO @FKItemSections_ResellersIdTable(id, resellers_id)
		SELECT TOP(@FKItemSections_TotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()) AS id,
		                                        subq.resellers_id AS resellers_id 
		FROM @tempFKItemSections_ResellersIdTable subq
		CROSS APPLY (SELECT TOP(@FKItemSections_TotalRowCnt / (SELECT COUNT(*) + 1 FROM @tempFKItemSections_ResellersIdTable)) 
		             resellers_id FROM @tempFKItemSections_ResellersIdTable) subq2
	SELECT * FROM @FKItemSections_ResellersIdTable ORDER BY resellers_id
	SELECT COUNT(*) AS FKItemSectionsResellersIdTableCnt FROM @FKItemSections_ResellersIdTable
	SELECT TOP(1000) * FROM @FKItemSections_ResellersIdTable ORDER BY resellers_id

	--5. create table for RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	--we want between 5 and 15 item sections for every reseller
	SET @MinPctCnt = 5.0
	SET @MaxPctCnt = 15.0
	SET @RandPct = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @BatchRowSize = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_RESELLERS)
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		--todo:  perhaps DOP issue here with this code as well similar to before?
		INSERT INTO RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS(id, created_at, is_available, name, resellers_id, updated_at)
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize AS id, col2.val AS created_at, col3.val AS is_available, 
			                          col4.val AS name, col5.resellers_id AS resellers_id, col6.val AS updated_at
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_DATETIMES col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col4 ON col1.id = col4.id + @col3IdOffset
			JOIN @FKItemSections_ResellersIdTable col5 ON col1.id = col5.id
			JOIN ##TEMP_ROWSET_DATETIMES col6 ON col1.id = col6.id + @col5IdOffset
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	SELECT COUNT(*) AS itemsectionsCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS 
	SELECT TOP (1000) * FROM RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS  ORDER BY resellers_id -- WHERE id>500000 order by id


	--5. setup table for RESELLERS_2ND_HAND_STUFF_ITEMS
	--avg 5 to 15 items per section (also get the resellers_id from it)
	DECLARE @FKItems_TotalRowCnt INT = 0
	SET @MinPctCnt = 5.0
	SET @MaxPctCnt = 15.0
	SET @RandPct = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @FKItems_TotalRowCnt = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS)
	DECLARE @tempFKItems_ItemSectionsIdTable TABLE (id INT, item_sections_id INT, resellers_id INT)
	DECLARE @FKItems_ItemSectionsIdTable TABLE (id INT, item_sections_id INT, resellers_id INT)
	DELETE FROM @tempFKItems_ItemSectionsIdTable
	INSERT INTO @tempFKItems_ItemSectionsIdTable(id, item_sections_id, resellers_id)
		SELECT 0, id AS item_sections_id, resellers_id AS resellers_id FROM RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS ORDER BY NEWID()
	SELECT @FKItems_TotalRowCnt AS FKItems_TotalRowCnt, 
	       (SELECT COUNT(*) + 1 FROM @tempFKItems_ItemSectionsIdTable) AS itemSectionsIdTableCnt
    (SELECT COUNT(*) + 1 FROM @tempFKItems_ItemSectionsIdTable GROUP BY item_sections_id)
	SELECT COUNT(*) AS tempFKItemsItemSectionsIdTableCnt FROM @tempFKItemSections_ResellersIdTable
	SELECT TOP(1000) * FROM @tempFKItems_ItemSectionsIdTable ORDER by resellers_id
	
	DELETE FROM @FKItems_ItemSectionsIdTable
	INSERT INTO @FKItems_ItemSectionsIdTable(id, item_sections_id, resellers_id)
		SELECT TOP(@FKItems_TotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()) AS id,
		                                 subq.item_sections_id AS item_sections_id,
		                                 subq.resellers_id AS resellers_id 
		FROM @tempFKItems_ItemSectionsIdTable subq
		CROSS APPLY (SELECT TOP(@FKItems_TotalRowCnt / (SELECT COUNT(*) + 1 FROM @tempFKItems_ItemSectionsIdTable)) 
		             item_sections_id, resellers_id FROM @tempFKItems_ItemSectionsIdTable) subq2
	SELECT * FROM @FKItems_ItemSectionsIdTable ORDER BY resellers_id
	SELECT COUNT(*) AS FKItemsItemSectionsIdTableCnt FROM @FKItems_ItemSectionsIdTable
	SELECT TOP(1000) * FROM @FKItems_ItemSectionsIdTable ORDER BY resellers_id
		
	SET @TotalLoops = 1 
	SET @TotalLoopsIdx = 0
	--have between 10 and 20 items for every item section
	SET @MinPctCnt = 10.0
	SET @MaxPctCnt = 20.0
	SET @RandPct = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @BatchRowSize = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS)
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col10IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col11IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_ITEMS(id, created_at, description, is_available, item_exists, section_id,
										           name, price, req_cust, resellers_id, string_id, updated_at)
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize AS id, col2.val AS created_at, col3.val AS description, 
			                          col4.val AS is_available, col5.val AS item_exists, col6col10.item_sections_id AS section_id, 
									  col7.val AS name, col8.val AS price, col9.val AS req_cust, 
									  col6col10.resellers_id AS resellers_id, col11.val AS string_id, col12.val as created_at
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_DATETIMES col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col4 ON col1.id = col4.id + @col3IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col5 ON col1.id = col5.id + @col4IdOffset
			JOIN @FKItems_ItemSectionsIdTable col6col10 ON col1.id = col6col10.id
			JOIN ##TEMP_ROWSET_TEMPWORDS col7 ON col1.id = col7.id + @col6IdOffset
			JOIN ##TEMP_ROWSET_FLOATS_MED col8 ON col1.id = col8.id + @col7IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col9 ON col1.id = col9.id + @col8IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col11 ON col1.id = col11.id + @col10IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col12 ON col1.id = col12.id + @col11IdOffset
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	SELECT TOP(1000) * FROM RESELLERS_2ND_HAND_STUFF_ITEMS ORDER BY resellers_id, section_id  -- WHERE id>500000 order by id
	SELECT COUNT(*) AS itemsCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMS 

	--6.)  setup RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER table
	--todo: not all items will have been ordered AND also for now only one item in each order
	--      later on consider having multiple items in a single order
	DECLARE @FKItemsInOrder_TotalRowCnt INT = 0
	SET @MinPctCnt = 0.15
	SET @MaxPctCnt = 0.50
	SET @RandPct = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @FKItemsInOrder_TotalRowCnt = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS)
	DECLARE @tempFKItemsInOrder_ItemsIdTable TABLE (id INT, item_id INT, resellers_id INT)
	DECLARE @FKItemsInOrder_ItemsIdTable TABLE (id INT, item_id INT, resellers_id INT)
	DELETE FROM @tempFKItemsInOrder_ItemsIdTable
	INSERT INTO @tempFKItemsInOrder_ItemsIdTable(id, item_id, resellers_id)
		SELECT 0, id AS item_id, resellers_id AS resellers_id FROM RESELLERS_2ND_HAND_STUFF_ITEMS ORDER BY NEWID()
	SELECT @FKItemsInOrder_TotalRowCnt AS FKItemsInOrder_TotalRowCnt, 
	       (SELECT COUNT(*) + 1 FROM @tempFKItemsInOrder_ItemsIdTable) AS itemsIdTableCnt
    SELECT COUNT(*) + 1 AS itemIdCnt FROM @tempFKItemsInOrder_ItemsIdTable GROUP BY item_id
	SELECT COUNT(*) AS tempFKItemsInOrderItemsIdTableCnt FROM @tempFKItemsInOrder_ItemsIdTable
	SELECT TOP(1000) * FROM @tempFKItemsInOrder_ItemsIdTable ORDER by item_id
	
	DELETE FROM @FKItemsInOrder_ItemsIdTable
	INSERT INTO @FKItemsInOrder_ItemsIdTable(id, item_id, resellers_id)
		SELECT TOP(@FKItemsInOrder_TotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()) AS id,
		                                 subq.item_id AS item_id,
		                                 subq.resellers_id AS resellers_id 
		FROM @tempFKItemsInOrder_ItemsIdTable subq
		--note:  dont need to cross apply this since @FKItemsInOrder_TotalRowCnt is < total count in ItemsIdTable
		--CROSS APPLY (SELECT TOP((SELECT COUNT(*) + 1 FROM @tempFKItemsInOrder_ItemsIdTable) / @FKItemsInOrder_TotalRowCnt) 
		--             item_id, resellers_id FROM @tempFKItemsInOrder_ItemsIdTable) subq2
	
	SELECT * FROM @FKItemsInOrder_ItemsIdTable ORDER BY item_id
	SELECT COUNT(*) AS FKItemsInOrderItemsIdTableCnt FROM @FKItemsInOrder_ItemsIdTable
	SELECT TOP(1000) * FROM @FKItemsInOrder_ItemsIdTable ORDER BY item_id

	SET @BatchRowSize = (SELECT COUNT(*) + 1 FROM @FKItemsInOrder_ItemsIdTable)
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER(id, order_id, count, pickup_status, 
		                                                    item_cost, item_price, item_id, resellers_id)
			SELECT TOP(@BatchRowSize) @TotalLoopsIdx * @BatchRowSize + col1.id AS id, NULL AS order_id, col3.val AS count,
			                          col4.val AS pickup_status, 
									  CAST((CAST(100*RAND(ABS(CHECKSUM(NEWID()))) AS INT) % 20 + 60) / 100.0 AS FLOAT) * col5.val AS item_cost,
									  --item_cost will be between 60 and 80% of the item_price
									  col5.val AS item_price, 
									  col6col7.item_id AS item_id, col6col7.resellers_id AS resellers_id
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			--order_id starts off hard coded as NULL, update it later once orders are created
			--JOIN ##TEMP_ROWSET_ALL_UNIQUE_INTS col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_SINGLE_INTS col3 ON col1.id = col3.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col4 ON col1.id = col4.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_FLOATS_MED col5 ON col1.id = col5.id + @col3IdOffset
			JOIN @FKItemsInOrder_ItemsIdTable col6col7 ON col1.id = col6col7.id
			
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	SELECT COUNT(*) AS itemsInOrderCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER 
	SELECT TOP(1000) * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER

	--7. create the table for the ORDERS
	--generate the user ids which will be used in the RESELLERS_2ND_HAND_STUFF_ORDERS
	--in our case, we will have between 1/3 and 1/5 of users each have an order
	DECLARE @FKOrders_TotalUsersRowCnt INT = 0
	SET @MinPctCnt = 0.20
	SET @MaxPctCnt = 0.33
	SET @RandPct = RAND(ABS(CHECKSUM(NEWID()))) * (@MaxPctCnt - @MinPctCnt) + @MinPctCnt
	SET @FKOrders_TotalUsersRowCnt = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_USERS) 
	DECLARE @tempFKOrders_UserIdTable TABLE (id INT, user_id INT)
	DECLARE @FKOrders_UserIdTable TABLE (id INT, user_id INT)
	DELETE FROM @tempFKOrders_UserIdTable
	INSERT INTO @tempFKOrders_UserIdTable(id, user_id)
		SELECT 0, id AS user_id FROM RESELLERS_2ND_HAND_STUFF_USERS ORDER BY NEWID()
	SELECT COUNT(*) AS tempFKOrdersUserIdTableCnt FROM @tempFKOrders_UserIdTable
	SELECT TOP(1000) * FROM @tempFKOrders_UserIdTable
	--reuse some of the unique user_ids when creating the orders, i.e. some users create multiple orders
	DELETE FROM @FKOrders_UserIdTable
	INSERT INTO @FKOrders_UserIdTable(id, user_id)
		SELECT TOP(@FKOrders_TotalUsersRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()),
		                                       subq.user_id 
		FROM @tempFKOrders_UserIdTable subq
		--we dont have to do a cross apply / SELF join 
		--since the number of rows in tempFKOrders_userIdTable > FKOrders_TotalUserRowCNT
		--CROSS APPLY (SELECT TOP(@FKOrders_TotalUsersRowCnt / (SELECT COUNT(*) + 1 FROM @tempFKOrders_UserIdTable)) 
		--             user_id FROM @tempFKOrders_UserIdTable) subq2
	SELECT COUNT(*) AS fkOrdersUserIdTableCnt FROM @FKOrders_UserIdTable
	SELECT TOP(1000) * FROM @FKOrders_UserIdTable ORDER BY id

	--1 to 1 relation, for every one order there is only one items records containing the items for that order
	--todo:  simpler case is only 1 item in every order_id, but I think it realistically should be 2-7 items
	DECLARE @FKOrders_ItemsInOrderTotalRowCnt INT = (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER)
	SELECT @FKOrders_ItemsInOrderTotalRowCnt,
		   (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER)
	SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER WHERE order_id IS NOT NULL
	SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER WHERE order_id IS NULL

	DECLARE @FKOrders_ItemsInOrderIdTable TABLE(id INT, items_in_order_id INT)
	DELETE FROM @FKOrders_ItemsInOrderIdTable
	INSERT INTO @FKOrders_ItemsInOrderIdTable(id, items_in_order_id)
		SELECT TOP(@FKOrders_ItemsInOrderTotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()),
		                                              id AS items_in_order_id
		FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
		WHERE order_id IS NULL --other rows have already been updated

	SELECT COUNT(*) AS fkOrdersItemsInOrderIdTableCnt FROM @FKOrders_ItemsInOrderIdTable
	SELECT TOP(1000) * FROM @FKOrders_ItemsInOrderIdTable ORDER BY items_in_order_id


	SET @BatchRowSize = @FKOrders_ItemsInOrderTotalRowCnt
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col10IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col11IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col12IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col13IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col14IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col15IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col16IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col17IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col19IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_ORDERS(id, created_at, discount_percent, item_price, items_in_order_id,
		                                            nonce, payment_status, price, service_fee, status, time_slot, updated_at, 
													user_id, user_ordering_location_address1, user_ordering_location_city,
													user_ordering_location_location, user_ordering_location_place,
													user_ordering_location_state, user_ordering_location_zip)
			SELECT TOP(@BatchRowSize) @TotalLoopsIdx * @BatchRowSize + col1.id AS id, col2.val AS created_at, col3.val AS discount_percent,
			                          col4.val AS item_price, col5.items_in_order_id AS items_in_order_id, col6.val AS nonce,
			                          col7.val AS payment_status, col8.val AS price, col9.val AS service_fee, col10.val AS status, 
									  col11.val AS time_slot, DATEADD(DAY, 1, col2.val) AS updated_at, col13.user_id AS user_id,
									  col14.val AS user_ordering_location_address1, col15.city AS user_ordering_location_city, 
									  col16.val AS user_ordering_location_location, col17.val AS user_ordering_location_place, 
									  col18.state_val AS user_ordering_location_state, col19.zip_code AS user_ordering_location_zip
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_DATETIMES col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_FLOATS_MED col4 ON col1.id = col4.id + @col3IdOffset
			JOIN @FKOrders_ItemsInOrderIdTable col5 ON col1.id = col5.id + @col4IdOffset
			JOIN ##TEMP_ROWSET_INTS_4_DIGITS col6 ON col1.id = col6.id + @col5IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col7 ON col1.id = col7.id + @col7IdOffset
			JOIN ##TEMP_ROWSET_FLOATS_MED col8 ON col1.id = col8.id + @col8IdOffset
			JOIN ##TEMP_ROWSET_FLOATS_TINY col9 ON col1.id = col9.id + @col9IdOffset
			JOIN ##TEMP_ROWSET_BOOLS col10 ON col1.id = col10.id + @col10IdOffset
			JOIN ##TEMP_ROWSET_INTS_4_DIGITS col11 ON col1.id = col11.id + @col11IdOffset
			--updated_at flag is derived from created_at flag
			--JOIN ##TEMP_ROWSET_DATETIMES col12 ON col1.id = col12.id + @col12IdOffset
			JOIN @FKOrders_UserIdTable col13 ON col1.id = col13.id + @col13IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col14 ON col1.id = col14.id + @col14IdOffset
			JOIN ##TEMP_ROWSET_CITIES col15 ON col1.id = col15.id + @col15IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col16 ON col1.id = col16.id + @col16IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col17 ON col1.id = col17.id + @col17IdOffset
			JOIN ##TEMP_ROWSET_STATES col18 ON col1.id = col18.id + @col18IdOffset
			JOIN ##TEMP_ROWSET_ZIPS col19 ON col1.id = col19.id + @col19IdOffset
			--WHERE col3.val <= 50

		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END
	--special case, put ceiling at 50 for all discount_percent
	UPDATE RESELLERS_2ND_HAND_STUFF_ORDERS SET discount_percent = 50 
	WHERE discount_percent > 50
	
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_ORDERS order by items_in_order_id
	SELECT COUNT(*) AS ordersCnt FROM RESELLERS_2ND_HAND_STUFF_ORDERS 

	--now that orders table creation completed, we have to go back and update the order_id in the ItemsInOrder table
	DECLARE @tempFKItemsInOrder_OrderIdTable TABLE (order_id INT, items_in_order_id INT)
	DELETE FROM @tempFKItemsInOrder_OrderIdTable
	INSERT INTO @tempFKItemsInOrder_OrderIdTable(order_id, items_in_order_id)
		SELECT id AS order_id, items_in_order_id AS items_in_order_id FROM RESELLERS_2ND_HAND_STUFF_ORDERS
		WHERE items_in_order_id IN (SELECT items_in_order_id FROM @FKOrders_ItemsInOrderIdTable)
	--SELECT * FROM @tempFKItemsInOrder_OrderIdTable ORDER BY items_in_order_id
	--SELECT COUNT(*) FROM @tempFKItemsInOrder_OrderIdTable
	--SELECT * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER ORDER BY id
	--SELECT * FROM @tempFKItemsInOrder_OrderIdTable ORDER BY items_in_order_id

	--now we have the mapping, go back to ITEMS_IN_ORDER table to update the order_id in it
	MERGE INTO RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER destT
	USING @tempFKItemsInOrder_OrderIdTable sourceT
	ON destT.id = sourceT.items_in_order_id
	WHEN MATCHED THEN
		UPDATE SET destT.order_id  = sourceT.order_id;
	
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER ORDER BY order_id
	SELECT COUNT(*) AS itemsInOrderCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER

	--8. create the table for RESELLERS_2ND_HAND_STUFF_TOKENS
	--generate 1 token for every 1 user_id
	DECLARE @FKTokens_UsersTotalRowCnt INT = (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_USERS)
	DECLARE @FKTokens_UserIdTable TABLE(id INT, user_id INT)
	DELETE FROM @FKTokens_UserIdTable
	INSERT INTO @FKTokens_UserIdTable(id, user_id)
		SELECT TOP(@FKTokens_UsersTotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()),
		                                       id AS user_id
		FROM RESELLERS_2ND_HAND_STUFF_USERS
	SELECT COUNT(*) AS fkTokensUserIdTableCnt FROM @FKTokens_UserIdTable
	SELECT TOP(1000) * FROM @FKTokens_UserIdTable ORDER BY user_id

	SET @BatchRowSize = @FKTokens_UsersTotalRowCnt
	SET @TotalLoops = 1
	SET @TotalLoopsIdx = 0
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
		INSERT INTO RESELLERS_2ND_HAND_STUFF_TOKENS(id, blacklisted, created_at, expires, token, updated_at, user_name, user_id)
			SELECT TOP(@BatchRowSize) @TotalLoopsIdx * @BatchRowSize + col1.id AS id, col2.val AS blacklisted, col3.val AS created_at, 
			                          DATEADD(DAY, 7, col3.val) AS expires, CONCAT(col5a.val, col5b.val) AS token, 
									  DATEADD(DAY, 10, col3.val) AS updated_at, col7.val AS user_name, col8.user_id AS user_id
			FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
			JOIN ##TEMP_ROWSET_BOOLS col2 ON col1.id = col2.id + @col1IdOffset
			JOIN ##TEMP_ROWSET_DATETIMES col3 ON col1.id = col3.id + @col2IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col5a ON col1.id = col5a.id + @col3IdOffset
			JOIN ##TEMP_ROWSET_INTS_4_DIGITS col5b ON col1.id = col5b.id + @col4IdOffset
			JOIN ##TEMP_ROWSET_TEMPWORDS col7 ON col1.id = col7.id + @col5IdOffset
			JOIN @FKTokens_UserIdTable col8 ON col1.id = col8.id + @col6IdOffset
		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END

	--todo:  decide whether I should move this code to a stored procedure which can be used 
	--       to add more orders to the OLTP db on a semi-daily basis or can be used to do retention curve building
	--to make ITEMS_IN_ORDER and ORDERS tables appear to be more realistic
	--we will increase the orders over time, but with the same users ordering
	--this will help create the appearance of negative churn of the same users ordering 
	--more and more as they become more satisified with the platform
	--also NOTE:  that if we wanted to simulate a platform with data updating on 
	--            some frequency then we could call this sp with different params somehow
	DECLARE @MIN_TOTAL_ORDERS_TO_ADD INT = @MAX_STAGE_2_ROWS_CNT
	DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.01
	DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.03
	DECLARE @START_ORDER_DATE DATE = '01/01/2016'
	DECLARE @END_ORDER_DATE DATE = '08/31/2025'
	EXEC dbo.GENERATE_MORE_ITEMS_IN_ORDERS_AND_ORDERS @MIN_PCT_USERS_CNT, @MAX_PCT_USERS_CNT, 
													  @MIN_TOTAL_ORDERS_TO_ADD, @START_ORDER_DATE, @END_ORDER_DATE	
	
	--overall agg analysis
	SELECT COUNT(*), created_at FROM RESELLERS_2ND_HAND_STUFF_ORDERS
	GROUP BY created_at, DATEPART(HOUR, created_at)
	ORDER BY created_at

	--find only the orders created by the sloping curve
	SELECT COUNT(*) AS ordersFromZeroHr FROM RESELLERS_2ND_HAND_STUFF_ORDERS
	WHERE DATEPART(HOUR, created_at) = 0
	
	SELECT COUNT(*) AS ordersFromNonzeroHr FROM RESELLERS_2ND_HAND_STUFF_ORDERS
	WHERE DATEPART(HOUR, created_at) <> 0
END


--todo:  use this to test separately
DECLARE @MIN_TOTAL_ORDERS_TO_ADD INT = 100000
DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.01
DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.03
DECLARE @START_ORDER_DATE DATE = '01/01/2016'
DECLARE @END_ORDER_DATE DATE = '08/31/2025'
EXEC dbo.GENERATE_MORE_ITEMS_IN_ORDERS_AND_ORDERS @MIN_PCT_USERS_CNT, @MAX_PCT_USERS_CNT, 
                                                  @MIN_TOTAL_ORDERS_TO_ADD, @START_ORDER_DATE, @END_ORDER_DATE

--todo:  there is some kind of bug where iio count doesnt equal orders count...
SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ORDERS
SELECT COUNT(*), created_at FROM RESELLERS_2ND_HAND_STUFF_ORDERS
GROUP BY created_at
ORDER BY created_at

SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
WHERE resellers_id IS NULL


SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
WHERE order_id IS NULL
SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
WHERE order_id IS NOT NULL
SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_RESELLERS

SELECT * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
where order_id IS NULL
SELECT TOP(100000) * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER


CREATE OR ALTER PROCEDURE dbo.GENERATE_MORE_ITEMS_IN_ORDERS_AND_ORDERS
	@MIN_PCT_USERS_CNT FLOAT,
	@MAX_PCT_USERS_CNT FLOAT,
	@MIN_TOTAL_ORDERS_TO_ADD INT,
	@START_ORDER_DATE DATE,
	@END_ORDER_DATE DATE
WITH EXECUTE AS OWNER
AS
BEGIN

	--todo: remove this, only keep this here for testing ONLY the data created by this stored proc
	--TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ORDERS
	--TRUNCATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
	DECLARE @BatchRowSize INT = 0
	DECLARE @col1IdOffset INT = 0
	DECLARE @col2IdOffset INT = 0
	DECLARE @col3IdOffset INT = 0
	DECLARE @col4IdOffset INT = 0
	DECLARE @col5IdOffset INT = 0
	DECLARE @col6IdOffset INT = 0
	DECLARE @col7IdOffset INT = 0
	DECLARE @col8IdOffset INT = 0
	DECLARE @col9IdOffset INT = 0
	DECLARE @col10IdOffset INT = 0
	DECLARE @col11IdOffset INT = 0
	DECLARE @col12IdOffset INT = 0
	DECLARE @col13IdOffset INT = 0
	DECLARE @col14IdOffset INT = 0
	DECLARE @col15IdOffset INT = 0
	DECLARE @col16IdOffset INT = 0
	DECLARE @col17IdOffset INT = 0
	DECLARE @col18IdOffset INT = 0
	DECLARE @col19IdOffset INT = 0
	DECLARE @col20IdOffset INT = 0
	DECLARE @col21IdOffset INT = 0
	DECLARE @col22IdOffset INT = 0
	DECLARE @col23IdOffset INT = 0
	DECLARE @col24IdOffset INT = 0
	--create a temp id offset table so that when generating the final records, our JOINs ids will be different for each loop
	DECLARE @tempIdOffsetTable TABLE(id INT)
	INSERT INTO @tempIdOffsetTable(id)
		SELECT val FROM ##TEMP_SINGLE_INT_TABLE
	SELECT * FROM @tempIdOffsetTable
	
	--this slope calculation with two temp table variables and 10million rows took ONLY 38s (so its scaling reasonable)
	--DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.01
	--DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.03
	--DECLARE @MIN_TOTAL_ORDERS_TO_ADD INT = 100000
	--DECLARE @START_ORDER_DATE DATE = '01/01/2016'
	--DECLARE @END_ORDER_DATE DATE = '08/31/2025'
	DECLARE @TotalDays INT = DATEDIFF(DAY, @START_ORDER_DATE, @END_ORDER_DATE)
	DECLARE @TotalOrdersPerDaySlope FLOAT = @MIN_TOTAL_ORDERS_TO_ADD / @TotalDays
	SELECT @START_ORDER_DATE, @END_ORDER_DATE, @TotalDays

	DECLARE @OrdersDateLookup TABLE(id INT, cur_date DATE, const_slope_orders_to_add INT, mod_slope FLOAT, orders_to_add INT)
	INSERT INTO @OrdersDateLookup(id, cur_date, const_slope_orders_to_add, mod_slope, orders_to_add)
		SELECT TOP(@TotalDays) id, DATEADD(DAY, id, @START_ORDER_DATE),
		                       id + id * @TotalOrdersPerDaySlope,
							   id * ((@TotalOrdersPerDaySlope + CAST(id/200.0 AS FLOAT)) / id),
							   id * id * ((@TotalOrdersPerDaySlope + CAST(id/200.0 AS FLOAT)) / id)
		FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS 
		ORDER BY id

	SELECT COUNT(*) ordersDateLookupCnt FROM @OrdersDateLookup
	SELECT * FROM @OrdersDateLookup ORDER BY id	
	--id * @TOTAL_ORDERS_PER_DAY_SLOPE:  this gives me a linear progression which may not be what i really want?
	--my first thought was my row counts would be like 28, 29, 30, 31, 32, etc
	--my first attempt was constant slope row counts 28, 56, 84, 112....
	--update:  created a mod_slope which ramps up slowly as the days progress

	DECLARE @fudge_factor FLOAT = 1.2
	DECLARE @corr_slope FLOAT = @fudge_factor * CAST(@MIN_TOTAL_ORDERS_TO_ADD AS FLOAT) / (SELECT MAX(orders_to_add) FROM @OrdersDateLookup) 
	DECLARE @FinalOrdersDateLookup TABLE(id INT, cur_date DATE)
	INSERT INTO @FinalOrdersDateLookup(id, cur_date)
		SELECT ROW_NUMBER() OVER (ORDER BY outerq.id), outerq.cur_date
		FROM @OrdersDateLookup outerq
		CROSS APPLY (SELECT TOP(CAST(@corr_slope * outerq.mod_slope AS INT)) * FROM @OrdersDateLookup) innerq

	SELECT COUNT(*) FROM @FinalOrdersDateLookup
	SELECT TOP(50000) * FROM @FinalOrdersDateLookup ORDER BY cur_date
	SELECT cur_date AS cur_date, COUNT(*) AS cntByDate FROM @FinalOrdersDateLookup GROUP BY cur_date ORDER BY cur_date
	DECLARE @finalOrdersCnt INT = (SELECT COUNT(*) FROM @FinalOrdersDateLookup)
	SELECT @finalOrdersCnt AS finalOrdersCnt, @corr_slope AS corr_slope

	--setup RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER table
	--todo: not all items will have been purchased AND also for now only one item in each order
	--      later on consider having multiple items in a single order
	DECLARE @FKItemsInOrder_TotalRowCnt INT = 0
	SET @FKItemsInOrder_TotalRowCnt = @finalOrdersCnt
	DECLARE @tempFKItemsInOrder_ItemsIdTable TABLE (id INT, item_id INT, resellers_id INT)
	DECLARE @FKItemsInOrder_ItemsIdTable TABLE (id INT, item_id INT, resellers_id INT)
	DELETE FROM @tempFKItemsInOrder_ItemsIdTable
	INSERT INTO @tempFKItemsInOrder_ItemsIdTable(id, item_id, resellers_id)
		SELECT 0, outerq.id AS item_id, outerq.resellers_id AS resellers_id
		FROM RESELLERS_2ND_HAND_STUFF_ITEMS outerq 
		CROSS APPLY (SELECT TOP(CAST(CEILING(CAST(@finalOrdersCnt AS FLOAT) / 
		                                     (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS)) AS INT)) * 
		             FROM RESELLERS_2ND_HAND_STUFF_ITEMS) innerq
		ORDER BY NEWID()
	
	SELECT @FKItemsInOrder_TotalRowCnt AS FKItemsInOrder_TotalRowCnt, 
	       (SELECT COUNT(*) + 1 FROM @tempFKItemsInOrder_ItemsIdTable) AS itemsIdTableCnt
    SELECT COUNT(*) + 1 AS itemIdCnt FROM @tempFKItemsInOrder_ItemsIdTable GROUP BY item_id
	SELECT COUNT(*) AS tempFKItemsInOrderItemsIdTableCnt FROM @tempFKItemsInOrder_ItemsIdTable
	SELECT TOP(1000) * FROM @tempFKItemsInOrder_ItemsIdTable ORDER by item_id
	
	DELETE FROM @FKItemsInOrder_ItemsIdTable
	INSERT INTO @FKItemsInOrder_ItemsIdTable(id, item_id, resellers_id)
		SELECT TOP(@FKItemsInOrder_TotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()) AS id,
		                                 subq.item_id AS item_id,
		                                 subq.resellers_id AS resellers_id 
		FROM @tempFKItemsInOrder_ItemsIdTable subq
		--note:  dont need to cross apply this since @FKItemsInOrder_TotalRowCnt is < total count in ItemsIdTable
		--CROSS APPLY (SELECT TOP((SELECT COUNT(*) + 1 FROM @tempFKItemsInOrder_ItemsIdTable) / @FKItemsInOrder_TotalRowCnt) 
		--             item_id, resellers_id FROM @tempFKItemsInOrder_ItemsIdTable) subq2
	
	SELECT * FROM @FKItemsInOrder_ItemsIdTable ORDER BY item_id
	SELECT COUNT(*) AS FKItemsInOrderItemsIdTableCnt FROM @FKItemsInOrder_ItemsIdTable
	SELECT TOP(1000) * FROM @FKItemsInOrder_ItemsIdTable ORDER BY item_id

	DECLARE @MaxItemsInOrderId INT = (SELECT MAX(id) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER) + 1
	IF @MaxItemsInOrderId IS NULL
		BEGIN	SET @MaxItemsInOrderId = 1	END
	SET @BatchRowSize = (SELECT COUNT(*) + 1 FROM @FKItemsInOrder_ItemsIdTable)

	SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	INSERT INTO RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER(id, order_id, count, pickup_status, 
		                                                item_cost, item_price, item_id, resellers_id)
		SELECT TOP(@BatchRowSize) @BatchRowSize + col1.id + @MaxItemsInOrderId AS id, 
			                        NULL AS order_id, col3.val AS count,
			                        col4.val AS pickup_status, 
									CAST((CAST(100*RAND(ABS(CHECKSUM(NEWID()))) AS INT) % 20 + 60) / 100.0 AS FLOAT) * col5.val AS item_cost,
									--item_cost will be between 60 and 80% of the item_price
									col5.val AS item_price, 
									col6col7.item_id AS item_id, col6col7.resellers_id AS resellers_id
		FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
		--order_id starts off hard coded as NULL, update it later once orders are created
		--JOIN ##TEMP_ROWSET_ALL_UNIQUE_INTS col2 ON col1.id = col2.id + @col1IdOffset
		JOIN ##TEMP_ROWSET_SINGLE_INTS col3 ON col1.id = col3.id + @col1IdOffset
		JOIN ##TEMP_ROWSET_BOOLS col4 ON col1.id = col4.id + @col2IdOffset
		JOIN ##TEMP_ROWSET_FLOATS_MED col5 ON col1.id = col5.id + @col3IdOffset
		JOIN @FKItemsInOrder_ItemsIdTable col6col7 ON col1.id = col6col7.id
			
	SELECT COUNT(*) AS itemsInOrderCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER 
	SELECT TOP(1000) * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER

	--create the table for the ORDERS
	--first, generate the user ids which will be used in the RESELLERS_2ND_HAND_STUFF_ORDERS
	DECLARE @FKOrders_TotalUsersRowCnt INT = 0
	--DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.10
	--DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.15
	--in this case, we will have between 1/5 and 1/3 of users each have an order
	DECLARE @RandPct FLOAT = RAND(ABS(CHECKSUM(NEWID()))) * (@MAX_PCT_USERS_CNT - @MIN_PCT_USERS_CNT) + @MIN_PCT_USERS_CNT
	SET @FKOrders_TotalUsersRowCnt = @RandPct * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_USERS)
	SELECT (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_USERS), @FKOrders_TotalUsersRowCnt

	DECLARE @tempFKOrders_UserIdTable TABLE (id INT, user_id INT)
	DECLARE @FKOrders_UserIdTable TABLE (id INT, user_id INT)
	DELETE FROM @tempFKOrders_UserIdTable
	INSERT INTO @tempFKOrders_UserIdTable(id, user_id)
		SELECT 0, id AS user_id FROM RESELLERS_2ND_HAND_STUFF_USERS ORDER BY NEWID()
	SELECT COUNT(*) AS tempFKOrdersUserIdTableCnt FROM @tempFKOrders_UserIdTable
	SELECT TOP(1000) * FROM @tempFKOrders_UserIdTable
	
	--DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.10
	--DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.15
	--DECLARE @RAND_PCT FLOAT = RAND(ABS(CHECKSUM(NEWID()))) * (@MAX_PCT_USERS_CNT - @MIN_PCT_USERS_CNT) + @MIN_PCT_USERS_CNT
	--DECLARE @FKOrders_TotalUsersRowCnt INT = @RAND_PCT * (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_USERS)
	--DECLARE @finalOrdersCnt INT = 128000

	SELECT @finalOrdersCnt AS finalOrdersCnt, 
	       @FKOrders_TotalUsersRowCnt AS FKOrders_TotalUsersRowCnt,
		   @finalOrdersCnt / @FKOrders_TotalUsersRowCnt AS totalUsers
	
	--reuse some of the unique user_ids when creating the orders, i.e. some users create multiple orders
	DELETE FROM @FKOrders_UserIdTable	
	INSERT INTO @FKOrders_UserIdTable(id, user_id)
		SELECT TOP(@finalOrdersCnt) ROW_NUMBER() OVER (ORDER BY NEWID()),
	            	                outerq.user_id 
		FROM @tempFKOrders_UserIdTable outerq
		CROSS APPLY (SELECT TOP(@finalOrdersCnt / @FKOrders_TotalUsersRowCnt) 
		             user_id FROM @tempFKOrders_UserIdTable) innerq
	SELECT COUNT(*) AS fkOrdersUserIdTableCnt FROM @FKOrders_UserIdTable
	SELECT TOP(1000) * FROM @FKOrders_UserIdTable ORDER BY id

	--1 to 1 relation, for every one order there is only one items records containing the items for that order
	--todo:  simpler case is only 1 item in every order_id, but I think it realistically should be 2-7 items
	--DECLARE @FKOrders_ItemsInOrderTotalRowCnt INT = (SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER)
	--DECLARE @finalOrdersCnt INT = (SELECT COUNT(*) FROM @FinalOrdersDateLookup)
	DECLARE @FKOrders_ItemsInOrderTotalRowCnt INT = @finalOrdersCnt
	SELECT @FKOrders_ItemsInOrderTotalRowCnt
	DECLARE @FKOrders_ItemsInOrderIdTable TABLE(id INT, items_in_order_id INT)
	DELETE FROM @FKOrders_ItemsInOrderIdTable
	INSERT INTO @FKOrders_ItemsInOrderIdTable(id, items_in_order_id)
		SELECT TOP(@FKOrders_ItemsInOrderTotalRowCnt) ROW_NUMBER() OVER (ORDER BY NEWID()),
		                                              id AS items_in_order_id
		FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
		WHERE order_id IS NULL --other rows have already been updated
	SELECT COUNT(*) AS fkOrdersItemsInOrderIdTableCnt FROM @FKOrders_ItemsInOrderIdTable
	SELECT TOP(1000) * FROM @FKOrders_ItemsInOrderIdTable ORDER BY items_in_order_id	

	DECLARE @MaxOrdersId INT = (SELECT MAX(id) FROM RESELLERS_2ND_HAND_STUFF_ORDERS) + 1
	IF @MaxOrdersId IS NULL
		BEGIN	SET @MaxOrdersId = 1	END
	SET @BatchRowSize = @FKOrders_ItemsInOrderTotalRowCnt
	
	SET @col1IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col2IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col3IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col4IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col5IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col6IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col7IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col8IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col9IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col10IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col11IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col12IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col13IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col14IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col15IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col16IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col17IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col18IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SET @col19IdOffset = (SELECT TOP(1) id FROM @tempIdOffsetTable ORDER BY NEWID())
	SELECT @finalOrdersCnt AS finalOrdersDateCnt,
			(SELECT COUNT(*) FROM @FKOrders_ItemsInOrderIdTable) AS fkOrdersItemsInOrderCnt,
		    (SELECT COUNT(*) FROM @FKOrders_UserIdTable) AS fkOrdersUserIdCnt
		       
	INSERT INTO RESELLERS_2ND_HAND_STUFF_ORDERS(id, created_at, discount_percent, item_price, items_in_order_id,
		                                        nonce, payment_status, price, service_fee, status, time_slot, updated_at, 
												user_id, user_ordering_location_address1, user_ordering_location_city,
												user_ordering_location_location, user_ordering_location_place,
												user_ordering_location_state, user_ordering_location_zip)
		SELECT TOP(@BatchRowSize)  @BatchRowSize + col1.id + @MaxOrdersId AS id, 
			                        col2.cur_date AS created_at, col3.val AS discount_percent,
			                        col4.val AS item_price, col5.items_in_order_id AS items_in_order_id, col6.val AS nonce,
			                        col7.val AS payment_status, col8.val AS price, col9.val AS service_fee, col10.val AS status, 
									col11.val AS time_slot, DATEADD(DAY, 1, col2.cur_date) AS updated_at, col13.user_id AS user_id,
									col14.val AS user_ordering_location_address1, col15.city AS user_ordering_location_city, 
									col16.val AS user_ordering_location_location, col17.val AS user_ordering_location_place, 
									col18.state_val AS user_ordering_location_state, col19.zip_code AS user_ordering_location_zip
		FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS col1
		JOIN @FinalOrdersDateLookup col2 ON col1.id = col2.id
		JOIN ##TEMP_ROWSET_INTS_2_DIGITS col3 ON col1.id = col3.id + @col2IdOffset
		JOIN ##TEMP_ROWSET_FLOATS_MED col4 ON col1.id = col4.id + @col3IdOffset
		JOIN @FKOrders_ItemsInOrderIdTable col5 ON col1.id = col5.id + @col4IdOffset
		JOIN ##TEMP_ROWSET_INTS_4_DIGITS col6 ON col1.id = col6.id + @col5IdOffset
		JOIN ##TEMP_ROWSET_BOOLS col7 ON col1.id = col7.id + @col7IdOffset
		JOIN ##TEMP_ROWSET_FLOATS_MED col8 ON col1.id = col8.id + @col8IdOffset
		JOIN ##TEMP_ROWSET_FLOATS_TINY col9 ON col1.id = col9.id + @col9IdOffset
		JOIN ##TEMP_ROWSET_BOOLS col10 ON col1.id = col10.id + @col10IdOffset
		JOIN ##TEMP_ROWSET_INTS_4_DIGITS col11 ON col1.id = col11.id + @col11IdOffset
		--updated_at flag is derived from created_at flag
		--JOIN ##TEMP_ROWSET_DATETIMES col12 ON col1.id = col12.id + @col12IdOffset
		JOIN @FKOrders_UserIdTable col13 ON col1.id = col13.id + @col13IdOffset
		JOIN ##TEMP_ROWSET_TEMPWORDS col14 ON col1.id = col14.id + @col14IdOffset
		JOIN ##TEMP_ROWSET_CITIES col15 ON col1.id = col15.id + @col15IdOffset
		JOIN ##TEMP_ROWSET_TEMPWORDS col16 ON col1.id = col16.id + @col16IdOffset
		JOIN ##TEMP_ROWSET_TEMPWORDS col17 ON col1.id = col17.id + @col17IdOffset
		JOIN ##TEMP_ROWSET_STATES col18 ON col1.id = col18.id + @col18IdOffset
		JOIN ##TEMP_ROWSET_ZIPS col19 ON col1.id = col19.id + @col19IdOffset
		--WHERE col3.val <= 50

	--special case, put ceiling at 50 for all discount_percent
	UPDATE RESELLERS_2ND_HAND_STUFF_ORDERS SET discount_percent = 50 
	WHERE discount_percent > 50
	
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_ORDERS order by items_in_order_id
	SELECT COUNT(*) AS ordersCnt FROM RESELLERS_2ND_HAND_STUFF_ORDERS 

	--now that orders table creation completed, we have to go back and update the order_id in the ItemsInOrder table
	DECLARE @tempFKItemsInOrder_OrderIdTable TABLE (order_id INT, items_in_order_id INT)
	DELETE FROM @tempFKItemsInOrder_OrderIdTable
	INSERT INTO @tempFKItemsInOrder_OrderIdTable(order_id, items_in_order_id)
		SELECT id AS order_id, items_in_order_id AS items_in_order_id FROM RESELLERS_2ND_HAND_STUFF_ORDERS
		WHERE items_in_order_id IN (SELECT items_in_order_id FROM @FKOrders_ItemsInOrderIdTable)
	--SELECT * FROM @tempFKItemsInOrder_OrderIdTable ORDER BY items_in_order_id
	--SELECT COUNT(*) FROM @tempFKItemsInOrder_OrderIdTable
	--SELECT * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER ORDER BY id
	--SELECT * FROM @tempFKItemsInOrder_OrderIdTable ORDER BY items_in_order_id

	--SELECT COUNT(*), items_in_order_id FROM @tempFKItemsInOrder_OrderIdTable 
	--GROUP BY items_in_order_id ORDER BY items_in_order_id DESC
	--HAVING COUNT(*) > 1
	SELECT COUNT(*), order_id FROM @tempFKItemsInOrder_OrderIdTable 
	GROUP BY order_id ORDER BY order_id DESC
	SELECT COUNT(*) FROM RESELLERS_2ND_HAND_STUFF_ORDERS GROUP BY id ORDER BY id DESC

	--now we have the mapping, go back to ITEMS_IN_ORDER table to update the order_id in it
	MERGE INTO RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER destT
	USING @tempFKItemsInOrder_OrderIdTable sourceT
	ON destT.id = sourceT.items_in_order_id
	WHEN MATCHED THEN
		UPDATE SET destT.order_id  = sourceT.order_id;
	
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER ORDER BY order_id 
	SELECT COUNT(*) AS itemsInOrderCnt FROM RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER

	--overall agg analysis
	SELECT COUNT(*), created_at FROM RESELLERS_2ND_HAND_STUFF_ORDERS
	GROUP BY created_at
	ORDER BY created_at

	--easily find the ones added just by this sp since the hour=0
	SELECT COUNT(*), created_at
	FROM RESELLERS_2ND_HAND_STUFF_ORDERS
	GROUP BY created_at, DATEPART(HOUR, created_at)
	ORDER BY created_at
END


--final code will be to use this code below
Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF
--2million resultant rows processed in 2m16s
DECLARE @MAX_STAGE_1_ROWS_CNT INT = 300000
DECLARE @MAX_STAGE_2_ROWS_CNT INT = @MAX_STAGE_1_ROWS_CNT / 2
EXEC dbo.GENERATE_TABLES_FOR_OLTP @MAX_STAGE_1_ROWS_CNT, @MAX_STAGE_2_ROWS_CNT

--add in some more orders along similar curve to make signal more visible
DECLARE @MIN_TOTAL_ORDERS_TO_ADD INT = @MAX_STAGE_2_ROWS_CNT
DECLARE @MIN_PCT_USERS_CNT FLOAT = 0.02
DECLARE @MAX_PCT_USERS_CNT FLOAT = 0.04
DECLARE @START_ORDER_DATE DATE = '01/01/2016'
DECLARE @END_ORDER_DATE DATE = '08/31/2025'
EXEC dbo.GENERATE_MORE_ITEMS_IN_ORDERS_AND_ORDERS @MIN_PCT_USERS_CNT, @MAX_PCT_USERS_CNT, 
                                                  @MIN_TOTAL_ORDERS_TO_ADD, @START_ORDER_DATE, @END_ORDER_DATE


--todo:   next things to do
--
--confirm data looks reasonable so check into github
--look over OLAP schema, maybe add things like customer name, or product name for analysis
--repopulate the OLAP schema
--build the Power BI metric tables