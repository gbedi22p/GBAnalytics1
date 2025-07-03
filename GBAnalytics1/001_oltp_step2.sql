--Create the store procedure and run it to generate the BIG temp tables of various global temp tables


--DROP PROCEDURE IF EXISTS dbo.GENERATE_FINAL_TEMP_DATA_FOR_OLTP

CREATE OR ALTER PROCEDURE dbo.GENERATE_FINAL_TEMP_DATA_FOR_OLTP
WITH EXECUTE AS OWNER
AS
BEGIN
	--cities 1 table variable
	--emails 1 table variable
	--floats 1 table > 500
	--floats 1 table < 10
	--floats 1 table 10 to 120
	--ints 1 table 2 digits
	--ints 1 table 3 digits
	--ints 1 table 4 digits
	--phoneno 1 table
	--single int 1 table
	--states int 1 table
	--tempwords varchar 1 table
	--zips 1 table
	--date 1 table
	--datetime 1 table
	DECLARE @BatchRowSize INT = 200000
	DECLARE @ColCnt INT = 0
	DECLARE @TotalColCnt INT = 0
	DECLARE @LoopIdx INT = 0
	DECLARE @LoopCnt INT = 0
	DECLARE @TotalLoopCnt INT = 0
	
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_CITY_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_CITIES
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_CITIES(id, city)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, city 
			FROM ##TEMP_CITY_TABLE ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) AS CitiesCnt FROM ##TEMP_ROWSET_CITIES
	SELECT * FROM ##TEMP_ROWSET_CITIES ORDER BY ID
	--SELECT COUNT(*) FROM ##TEMP_ROWSET_CITIES_TABLE
	--WHERE city='babsmaorm skoph'

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_EMAIL_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_EMAILS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_EMAILS(id, email)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, email 
			FROM ##TEMP_EMAIL_TABLE ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) as EmailsCount FROM ##TEMP_ROWSET_EMAILS
	SELECT * FROM ##TEMP_ROWSET_EMAILS ORDER BY ID
	

	--need temporary table for states to do faster cross joins then looping and adding
	DECLARE @TempFloatTable TABLE(id INT, val FLOAT)
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val < 10) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE intpart	
	--SELECT * FROM @TempFloatTable ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_TINY
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_FLOATS_TINY(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val 
			FROM @TempFloatTable
			ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--PRINT(@LoopCnt + ' ' + @TotalLoopCnt + ' ' + @LoopIdx)
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) AS TinyFloatsCount FROM ##TEMP_ROWSET_FLOATS_TINY
	SELECT * FROM ##TEMP_ROWSET_FLOATS_TINY ORDER BY ID

	DELETE FROM @TempFloatTable
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val > 10 AND val < 120) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE intpart	
	--SELECT * FROM @TempFloatTable ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_MED
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_FLOATS_MED(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val 
			FROM @TempFloatTable 
			ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) AS MedFloatsCount FROM ##TEMP_ROWSET_FLOATS_MED
	SELECT * FROM ##TEMP_ROWSET_FLOATS_MED ORDER BY ID	

	--need temporary table to give more granulatity to specific float vals precisions
	DELETE FROM @TempFloatTable
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val > 120) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
	--SELECT * FROM @TempFloatTable ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	--PRINT('ColCnt ' + CAST(@ColCnt AS NVARCHAR))
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_LARGE
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_FLOATS_LARGE(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val 
			FROM @TempFloatTable 
			ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--PRINT(@LoopCnt)
		--PRINT(@LoopCnt + ' ' + @TotalLoopCnt + ' ' + @LoopIdx)
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) AS LargeFloatsCnt FROM ##TEMP_ROWSET_FLOATS_LARGE
	SELECT * FROM ##TEMP_ROWSET_FLOATS_LARGE ORDER BY ID

	--create a temp table variable in order to reorder the IDs
	DECLARE @tempIntTable TABLE(id INT, val INT)
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=2

	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	--PRINT('ColCnt ' + CAST(@ColCnt AS NVARCHAR))
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_2_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_INTS_2_DIGITS(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val 
			FROM @tempIntTable 
			ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--PRINT(@LoopCnt)
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	SELECT COUNT(*) AS Ints2Cnt FROM ##TEMP_ROWSET_INTS_2_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_2_DIGITS ORDER BY ID

	--create a temp table variable in order to reorder the IDs
	DELETE FROM @tempIntTable
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=3

	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_3_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_INTS_3_DIGITS(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val
			FROM @tempIntTable
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS Ints3Cnt FROM ##TEMP_ROWSET_INTS_3_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_3_DIGITS ORDER BY ID

	--create a temp table variable in order to reorder the IDs
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=4

	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_4_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_INTS_4_DIGITS(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val
			FROM @tempIntTable
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS Ints4Digits FROM ##TEMP_ROWSET_INTS_4_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_4_DIGITS ORDER BY ID 

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_PHONE_NO_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_PHONE_NOS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_PHONE_NOS(id, phone_no, country_code)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, phone_no, country_code
			FROM ##TEMP_PHONE_NO_TABLE
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS PhoneNosCnt from ##TEMP_ROWSET_PHONE_NOS
	SELECT * FROM ##TEMP_ROWSET_PHONE_NOS ORDER BY ID

	--need temporary table for states to do faster cross joins then looping and adding
	DECLARE @tempStateTable TABLE(id INT, state NVARCHAR(50))
	INSERT INTO @tempStateTable(id, state)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), q.state FROM ##TEMP_STATE_TABLE q
		CROSS APPLY (SELECT * FROM ##TEMP_STATE_TABLE) subq
		CROSS APPLY (SELECT TOP(10) * FROM ##TEMP_STATE_TABLE) subq2
	--SELECT * FROM @tempStateTable ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM @tempStateTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_STATES(id, state)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, state
			FROM @tempStateTable
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS StatesCnt FROM ##TEMP_ROWSET_STATES
	SELECT * FROM ##TEMP_ROWSET_STATES ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_WORDS_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_TEMPWORDS(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val
			FROM ##TEMP_WORDS_TABLE
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS WordsCnt FROM ##TEMP_ROWSET_TEMPWORDS
	SELECT * FROM ##TEMP_ROWSET_TEMPWORDS ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_ZIP_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_ZIPS(id, zip_code)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, zip_code
			FROM ##TEMP_ZIP_TABLE
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS ZipsCnt FROM ##TEMP_ROWSET_ZIPS
	SELECT * FROM ##TEMP_ROWSET_ZIPS ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_DATE_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_DATES(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val
			FROM ##TEMP_DATE_TABLE
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS DatesCnt FROM ##TEMP_ROWSET_DATES
	SELECT * FROM ##TEMP_ROWSET_DATES ORDER BY id

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_DATETIME_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO ##TEMP_ROWSET_DATETIMES(id, val)
			SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val
			FROM ##TEMP_DATETIME_TABLE
			ORDER BY id
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT COUNT(*) AS DatetimesCnt FROM ##TEMP_ROWSET_DATETIMES
	SELECT * FROM ##TEMP_ROWSET_DATETIMES ORDER BY id

--todo:  is there a way to get a small table from 10 rows to 1m in an efficient way, like a CTE or something
--DROP TABLE IF EXISTS ##TEMP_ROWSET_SINGLE_INTS


	/*
	SELECT subq.id, subq.val, subq2.val, subq3.val, subq4.val 
	FROM @Col1 subq
	JOIN @Col2 subq2 ON subq.id=subq2.id
	JOIN @Col3 subq3 ON subq.id=subq3.id
	JOIN @Col4 subq4 ON subq.id=subq4.id
	ORDER BY subq.id
	PRINT(@TotalCol1Cnt)
	PRINT(@TotalCol2Cnt)
	PRINT(@TotalCol3Cnt)
	PRINT(@TotalCol4Cnt)
	*/
	--todo gkb
	--create temp tables up to BatchRowSize for all data types
	--then optionally include them into each table based on their needs
	--tricky part is 1 to 1 or 1 to Many keys between tables
	--create a Column for DATE and DATETIME objects
END


EXEC dbo.GENERATE_FINAL_TEMP_DATA_FOR_OLTP
