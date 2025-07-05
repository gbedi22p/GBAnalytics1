Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF


CREATE OR ALTER PROCEDURE dbo.GENERATE_DIM_TABLES_FOR_OLTP
WITH EXECUTE AS OWNER
AS
BEGIN

	DECLARE @TotalLoops INT = 1
	DECLARE @TotalLoopsIdx INT = 0
	DECLARE @BatchRowSize INT = 100000

	--dim tables
	DELETE FROM RESELLERS_2ND_HAND_STUFF_COUPONS
	WHILE @TotalLoopsIdx < @TotalLoops
	BEGIN
		INSERT INTO RESELLERS_2ND_HAND_STUFF_COUPONS(id, created_at, days_to_live, 
		                                             discount_percent, discount_price, minimum_order, name, updated_at, usage_limit)
			SELECT TOP(@BatchRowSize) col1.id + @TotalLoopsIdx * @BatchRowSize, col2.val,
			                          col3.val, col4.val, col5.val, col6.val, col7.val, col8.val, col9.val
			FROM ##TEMP_ROWSET_ALL_INTS col1
			JOIN ##TEMP_ROWSET_DATETIMES col2 ON col1.id = col2.id
			--todo:  add some level of randomness if using same table and/or
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col3 ON col1.id = col3.id
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col4 ON col1.id = col4.id
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col5 ON col1.id = col5.id
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col6 ON col1.id = col6.id
			JOIN ##TEMP_ROWSET_TEMPWORDS col7 ON col1.id = col7.id
			JOIN ##TEMP_ROWSET_DATETIMES col8 ON col1.id = col8.id
			JOIN ##TEMP_ROWSET_INTS_2_DIGITS col9 ON col1.id = col9.id
			ORDER BY NEWID()
			

		SET @TotalLoopsIdx = @TotalLoopsIdx + 1
	END

	--SELECT * FROM ##TEMP_INT_TABLE ORDER BY id
	--SELECT * FROM ##TEMP_ROWSET_DATETIMES ORDER BY id
	SELECT * FROM RESELLERS_2ND_HAND_STUFF_COUPONS ORDER BY id

/*
RESELLERS_2ND_HAND_STUFF_ITEMS
RESELLERS_2ND_HAND_STUFF_SECTIONS
RESELLERS_2ND_HAND_STUFF_RESELLERS
RESELLERS_2ND_HAND_STUFF_TAXRATES
RESELLERS_2ND_HAND_STUFF_USERS

--fact tables
RESELLERS_2ND_HAND_STUFF_ORDERS
RESELLERS_2ND_HAND_STUFF_TOKENS
*/

END


SET NOCOUNT OFF
EXEC dbo.GENERATE_DIM_TABLES_FOR_OLTP
