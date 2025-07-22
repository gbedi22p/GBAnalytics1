--1. execute these three statements first
Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF
DROP PROCEDURE [dbo].GENERATE_FINAL_TEMP_DATA_FOR_OLTP


--2.  create this procedure second
CREATE OR ALTER PROCEDURE [dbo].GENERATE_FINAL_TEMP_DATA_FOR_OLTP 
	@TEMP_ROWSET_FINAL_ROW_CNT INT
WITH EXECUTE AS OWNER
AS
BEGIN
	--STEP2 - SETUP ALL OF THE FINAL TEMP DATA WITH EQUAL NUMBER OF ROW COUNTS FOR ALL TEMP TABLES FOR FASTER INSERTION LATER
	--
	--DOP caused an issue earlier so this is way to check te DOP at the database level
	--however, from my earlier test, using this did not prevent the DOP from going into parallel way
	--ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0
	--SELECT [value] FROM sys.database_scoped_configurations WHERE [name] = 'MAXDOP';
	--ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1
	--SELECT [value] FROM sys.database_scoped_configurations WHERE [name] = 'MAXDOP';
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
	TRUNCATE TABLE ##TEMP_ROWSET_CITIES
	TRUNCATE TABLE ##TEMP_ROWSET_EMAILS
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_TINY
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_MED
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_LARGE
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_2_DIGITS
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_3_DIGITS
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_4_DIGITS
	TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS
	TRUNCATE TABLE ##TEMP_ROWSET_PHONE_NOS
	TRUNCATE TABLE ##TEMP_ROWSET_SINGLE_INTS
	TRUNCATE TABLE ##TEMP_ROWSET_STATES
	TRUNCATE TABLE ##TEMP_ROWSET_TEMPWORDS
	TRUNCATE TABLE ##TEMP_ROWSET_ZIPS
	TRUNCATE TABLE ##TEMP_ROWSET_DATES
	TRUNCATE TABLE ##TEMP_ROWSET_DATETIMES
	TRUNCATE TABLE ##TEMP_ROWSET_BOOLS
	TRUNCATE TABLE ##TEMP_ROWSET_URLS
	TRUNCATE TABLE ##TEMP_ROWSET_BUDGET_CATGS
	TRUNCATE TABLE ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS

	DECLARE @loopCntTable TABLE(loopIdx INT, loopRowsCntStart INT, loopRowsCntEnd INT)
	DECLARE @BatchRowSize INT = @TEMP_ROWSET_FINAL_ROW_CNT
	DECLARE @ColCnt INT = 0
	DECLARE @TotalColCnt INT = 0
	DECLARE @LoopIdx INT = 0
	DECLARE @LoopCnt INT = 0
	DECLARE @TotalLoopCnt INT = 0
	
	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_CITY_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END

	TRUNCATE TABLE ##TEMP_ROWSET_CITIES
	INSERT INTO ##TEMP_ROWSET_CITIES(id, city)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.city
		FROM ##TEMP_CITY_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS citiesCnt FROM ##TEMP_ROWSET_CITIES
	SELECT * FROM ##TEMP_ROWSET_CITIES ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_EMAIL_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	
	TRUNCATE TABLE ##TEMP_ROWSET_EMAILS
	INSERT INTO ##TEMP_ROWSET_EMAILS(id, email)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.email
		FROM ##TEMP_EMAIL_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS emailsCnt FROM ##TEMP_ROWSET_EMAILS
	SELECT * FROM ##TEMP_ROWSET_EMAILS ORDER BY id

	--need temporary table for states to do faster cross joins then looping and adding
	DECLARE @TempFloatTable TABLE(id INT, val FLOAT)
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val < 10) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
		--CROSS JOIN ##TEMP_SINGLE_INT_TABLE intpart	
	--SELECT * FROM @TempFloatTable ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_TINY
	INSERT INTO ##TEMP_ROWSET_FLOATS_TINY(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @TempFloatTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS tinyFloatsCnt FROM ##TEMP_ROWSET_FLOATS_TINY
	SELECT * FROM ##TEMP_ROWSET_FLOATS_TINY ORDER BY id

	DELETE FROM @TempFloatTable
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val > 10 AND val < 120) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
		--CROSS JOIN ##TEMP_SINGLE_INT_TABLE intpart	
	--SELECT * FROM @TempFloatTable ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_MED
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		--INSERT INTO ##TEMP_ROWSET_FLOATS_MED(id, val)
		--	SELECT TOP(@LoopCnt) @TotalLoopCnt + id, val 
		--	FROM @TempFloatTable 
		--	ORDER BY id
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	--SELECT COUNT(*) AS MedFloatsCount FROM ##TEMP_ROWSET_FLOATS_MED
	--SELECT * FROM ##TEMP_ROWSET_FLOATS_MED ORDER BY id	

	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_MED
	INSERT INTO ##TEMP_ROWSET_FLOATS_MED(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @TempFloatTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS MedFloatsCount FROM ##TEMP_ROWSET_FLOATS_MED
	SELECT * FROM ##TEMP_ROWSET_FLOATS_MED ORDER BY id	

	--need temporary table to give more granulatity to specific float vals precisions
	DELETE FROM @TempFloatTable
	INSERT INTO @TempFloatTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by subq.val), subq.val + CAST(fpart.val/100.0 + fpart2.val/1000.0 AS FLOAT) FROM 
		(SELECT val FROM ##TEMP_FLOAT_TABLE WHERE val > 120) subq
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE fpart2
	--SELECT * FROM @TempFloatTable ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @TempFloatTable)
	--PRINT('ColCnt ' + CAST(@ColCnt AS NVARCHAR))
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_LARGE
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	
	TRUNCATE TABLE ##TEMP_ROWSET_FLOATS_LARGE
	INSERT INTO ##TEMP_ROWSET_FLOATS_LARGE(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @TempFloatTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS LargeFloatsCnt FROM ##TEMP_ROWSET_FLOATS_LARGE
	SELECT * FROM ##TEMP_ROWSET_FLOATS_LARGE ORDER BY id

	--todo:  come back later and check if can skip thi step later on..soemhow
	--create a temp table variable in order to reorder the IDs
	DECLARE @tempIntTable TABLE(id INT, val INT)
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=2

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	--PRINT('ColCnt ' + CAST(@ColCnt AS NVARCHAR))
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_2_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET @TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
		--PRINT(@LoopCnt)
		--todo:  introduce some random errors like bad values, nulls, etc. perhaps..?
	END
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_2_DIGITS
	INSERT INTO ##TEMP_ROWSET_INTS_2_DIGITS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @tempIntTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS Ints2Cnt FROM ##TEMP_ROWSET_INTS_2_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_2_DIGITS ORDER BY ID

	--create a temp table variable in order to reorder the IDs
	DELETE FROM @tempIntTable
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=3

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_3_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	INSERT INTO ##TEMP_ROWSET_INTS_3_DIGITS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @tempIntTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS Ints3Cnt FROM ##TEMP_ROWSET_INTS_3_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_3_DIGITS ORDER BY ID

	/*DECLARE @loopCntTable TABLE(loopIdx INT, loopRowsCntStart INT, loopRowsCntEnd INT)
	DECLARE @tempIntTable TABLE(id INT, val INT)
	DECLARE @BatchRowSize INT = 1000000
	DECLARE @ColCnt INT = 0
	DECLARE @TotalColCnt INT = 0
	DECLARE @LoopIdx INT = 0
	DECLARE @LoopCnt INT = 0
	DECLARE @TotalLoopCnt INT = 0*/
	--create a temp table variable in order to reorder the IDs
	DELETE FROM @tempIntTable
	INSERT INTO @tempIntTable(id, val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), val FROM ##TEMP_INT_TABLE WHERE num_digits=4

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_INTS_4_DIGITS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	SELECT * FROM @loopCntTable
	INSERT INTO ##TEMP_ROWSET_INTS_4_DIGITS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @tempIntTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS Ints4Digits FROM ##TEMP_ROWSET_INTS_4_DIGITS
	SELECT * FROM ##TEMP_ROWSET_INTS_4_DIGITS ORDER BY id

	--Next Step:  setup the ##TEMP_ROWSET_SINGLE_INTS table
	--DECLARE @tempIntTable TABLE(id INT, val INT)
	DELETE FROM @tempIntTable
	INSERT INTO @tempIntTable (id, val)
		SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) as id, q1.val as val
		FROM ##TEMP_SINGLE_INT_TABLE q1
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE q2
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE q3
		CROSS JOIN ##TEMP_SINGLE_INT_TABLE q4
	SELECT * FROM @tempIntTable
	--SELECT COUNT(*) FROM @tempIntTable
	
	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempIntTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
			VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_SINGLE_INTS
	INSERT INTO ##TEMP_ROWSET_SINGLE_INTS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @tempIntTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
	SELECT COUNT(*) AS SingleIntsCnt FROM ##TEMP_ROWSET_SINGLE_INTS
	SELECT TOP(10000) * FROM ##TEMP_ROWSET_SINGLE_INTS ORDER BY id
	SELECT COUNT(*) as cntAs0
	FROM ##TEMP_ROWSET_SINGLE_INTS
	WHERE val=0
	
	TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS
	TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1

	--DECLARE @BatchRowSize INT = 500000
	--Need unique sequential integers for usage as the primary key in the final OLTP database
	DECLARE @DynamicSqlCmd NVARCHAR(MAX) = N''
	TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1
	TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS
	DECLARE @NumCrossApplyAllInts INT = LOG(@BatchRowSize, 10)
	DECLARE @NumCrossApplyAllIdx INT = 2
	SET @DynamicSqlCmd = CONCAT('INSERT INTO ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1(id)', CHAR(13),
	                            'SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) as id FROM ##TEMP_SINGLE_INT_TABLE', CHAR(13))
	SET @NumCrossApplyAllIdx = 1
	WHILE @NumCrossApplyAllIdx <= @NumCrossApplyAllInts
	BEGIN
		SET @DynamicSqlCmd = CONCAT(@DynamicSqlCmd, CHAR(13), 'CROSS JOIN ##TEMP_SINGLE_INT_TABLE subq', 
		                            CAST(@NumCrossApplyAllIdx AS NVARCHAR), CHAR(13))
		SET @NumCrossApplyAllIdx = @NumCrossApplyAllIdx + 1
	END
	PRINT(@DynamicSqlCmd)
	EXEC(@DynamicSqlCmd)
	SET @DynamicSqlCmd = 'TRUNCATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS' + CHAR(13) +
  	                     'INSERT INTO ##TEMP_ROWSET_ALL_UNIQUE_INTS(id, val)' + CHAR(13) +
		                 'SELECT id, id as val FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1'
	EXEC(@DynamicSqlCmd)
	--SELECT COUNT(*) FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS
	--SELECT * FROM ##TEMP_ROWSET_ALL_UNIQUE_INTS ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_PHONE_NO_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	TRUNCATE TABLE ##TEMP_ROWSET_PHONE_NOS
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	
	TRUNCATE TABLE ##TEMP_ROWSET_PHONE_NOS
	INSERT INTO ##TEMP_ROWSET_PHONE_NOS(id, phone_no)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.phone_no
		FROM ##TEMP_PHONE_NO_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT * FROM ##TEMP_ROWSET_PHONE_NOS ORDER BY id
	SELECT COUNT(*) AS phoneNosCnt FROM ##TEMP_ROWSET_PHONE_NOS

	--need temporary table variable for states to do faster cross joins instead of looping and adding
	DECLARE @tempStateTable TABLE(id INT, state_val NVARCHAR(50))
	INSERT INTO @tempStateTable(id, state_val)
		SELECT ROW_NUMBER() OVER (Order by NEWID()), q.state_val FROM ##TEMP_STATE_TABLE q
		CROSS APPLY (SELECT * FROM ##TEMP_STATE_TABLE) subq
		CROSS APPLY (SELECT TOP(10) * FROM ##TEMP_STATE_TABLE) subq2		
	--SELECT * FROM @tempStateTable ORDER BY id
	--SELECT COUNT(*) FROM @tempStateTable

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempStateTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the repeated amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(ABS(CHECKSUM(NEWID()))) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	--SELECT * FROM @loopCntTable ORDER By loopIdx

	--build final temp_rowset table, total rows cnt = @BatchRowSize
	TRUNCATE TABLE ##TEMP_ROWSET_STATES
	INSERT INTO ##TEMP_ROWSET_STATES(id, state_val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.state_val
		FROM @tempStateTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS StatesCnt FROM ##TEMP_ROWSET_STATES
	SELECT * FROM ##TEMP_ROWSET_STATES ORDER BY id
	--SELECT COUNT(*) AS bachshmoeltCnt FROM ##TEMP_ROWSET_STATES WHERE state_val='bachshmoelt'

	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_WORDS_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	DELETE FROM @loopCntTable
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END

	TRUNCATE TABLE ##TEMP_ROWSET_TEMPWORDS
	INSERT INTO ##TEMP_ROWSET_TEMPWORDS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM ##TEMP_WORDS_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS WordsCnt FROM ##TEMP_ROWSET_TEMPWORDS
	SELECT * FROM ##TEMP_ROWSET_TEMPWORDS ORDER BY id
	
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_ZIP_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	DELETE FROM @loopCntTable
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END

	TRUNCATE TABLE ##TEMP_ROWSET_ZIPS
	INSERT INTO ##TEMP_ROWSET_ZIPS(id, zip_code)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.zip_code
		FROM ##TEMP_ZIP_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS ZipsCnt FROM ##TEMP_ROWSET_ZIPS
	SELECT * FROM ##TEMP_ROWSET_ZIPS ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_DATE_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_DATES
	INSERT INTO ##TEMP_ROWSET_DATES(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM ##TEMP_DATE_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS DatesCnt FROM ##TEMP_ROWSET_DATES
	SELECT * FROM ##TEMP_ROWSET_DATES ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_DATETIME_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_DATETIMES
	INSERT INTO ##TEMP_ROWSET_DATETIMES(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM ##TEMP_DATETIME_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS DatetimesCnt FROM ##TEMP_ROWSET_DATETIMES
	SELECT * FROM ##TEMP_ROWSET_DATETIMES ORDER BY id
	
	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_BOOL_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 2 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_BOOLS
	INSERT INTO ##TEMP_ROWSET_BOOLS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM ##TEMP_BOOL_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS boolsCnt FROM ##TEMP_ROWSET_BOOLS
	SELECT * FROM ##TEMP_ROWSET_BOOLS ORDER BY id

	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM ##TEMP_URL_TABLE)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		--create some variability in the amount added for each loop
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
		VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_URLS
	INSERT INTO ##TEMP_ROWSET_URLS(id, url)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.url
		FROM ##TEMP_URL_TABLE subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
		--ORDER BY lct.loopIdx, tst.id
	SELECT COUNT(*) AS UrlsCnt FROM ##TEMP_ROWSET_URLS
	SELECT * FROM ##TEMP_ROWSET_URLS ORDER BY id

	DECLARE @tempNvarcharTable TABLE(id INT, val NVARCHAR(50))
	DELETE FROM @tempNvarcharTable
	INSERT INTO @tempNvarcharTable (id, val)
		SELECT ROW_NUMBER() OVER (ORDER BY q1.val) as id, q1.val as val
		FROM ##TEMP_BUDGET_CATG_TABLE q1
		CROSS JOIN ##TEMP_BUDGET_CATG_TABLE q2
		CROSS JOIN ##TEMP_BUDGET_CATG_TABLE q3
		CROSS JOIN ##TEMP_BUDGET_CATG_TABLE q4
		CROSS JOIN ##TEMP_BUDGET_CATG_TABLE q5
		CROSS JOIN ##TEMP_BUDGET_CATG_TABLE q6
	SELECT * FROM @tempNvarcharTable
	SELECT COUNT(*) FROM @tempNvarcharTable
	
	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempNvarcharTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
			   VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_BUDGET_CATGS
	INSERT INTO ##TEMP_ROWSET_BUDGET_CATGS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @TempNvarcharTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
	SELECT COUNT(*) AS BudgetCatgsCnt FROM ##TEMP_ROWSET_BUDGET_CATGS
	SELECT TOP(10000) * FROM ##TEMP_ROWSET_BUDGET_CATGS ORDER BY id

	DELETE FROM @tempNvarcharTable
	INSERT INTO @tempNvarcharTable (id, val)
		SELECT ROW_NUMBER() OVER (ORDER BY q1.val) as id, q1.val as val
		FROM ##TEMP_KIND_OF_BUSINESS_CATG_TABLE q1
		CROSS JOIN ##TEMP_KIND_OF_BUSINESS_CATG_TABLE q2
		CROSS JOIN ##TEMP_KIND_OF_BUSINESS_CATG_TABLE q3
		CROSS JOIN ##TEMP_KIND_OF_BUSINESS_CATG_TABLE q4
	SELECT * FROM @tempNvarcharTable
	SELECT COUNT(*) FROM @tempNvarcharTable
	
	DELETE FROM @loopCntTable
	SET @ColCnt = (SELECT COUNT(*) FROM @tempNvarcharTable)
	SET @LoopIdx = 0
	SET @TotalLoopCnt = 0
	WHILE @TotalLoopCnt < @BatchRowSize
	BEGIN
		SET @LoopCnt = @ColCnt / (CAST(100*RAND(CHECKSUM(NEWID())) AS INT) % 4 + 1)
		INSERT INTO @loopCntTable(loopIdx, loopRowsCntStart, loopRowsCntEnd) 
   	    	   VALUES(@LoopIdx, @TotalLoopCnt - 1, @TotalLoopCnt + @LoopCnt - 1)
		SET	@TotalLoopCnt = @TotalLoopCnt + @LoopCnt
		SET @LoopIdx = @LoopIdx + 1
	END
	TRUNCATE TABLE ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS
	INSERT INTO ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS(id, val)
		SELECT subq2.loopRowsCntStart + subq.id + 1 AS id, subq.val
		FROM @TempNvarcharTable subq
		CROSS APPLY (SELECT TOP(SELECT COUNT(*) FROM @loopCntTable) * FROM @loopCntTable) subq2
		WHERE subq2.loopRowsCntStart + subq.id <= subq2.loopRowsCntEnd
	SELECT COUNT(*) AS kindOfBusinessCatgsCnt FROM ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS
	SELECT TOP(10000) * FROM ##TEMP_ROWSET_KIND_OF_BUSINESS_CATGS ORDER BY id
END



--3.  finally execute these statemetns always as precondition to the stored procedure below as its needed by the dynamic sql usage
DROP TABLE IF EXISTS ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1
CREATE TABLE ##TEMP_ROWSET_ALL_UNIQUE_INTS_STEP1
(
	id INT
)
Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF
--1m final row cnt:  3m50s
DECLARE @TEMP_ROWSET_FINAL_ROW_CNT INT = 500000
EXEC dbo.GENERATE_FINAL_TEMP_DATA_FOR_OLTP @TEMP_ROWSET_FINAL_ROW_CNT

