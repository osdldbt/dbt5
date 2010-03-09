/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Trade Update transaction
 * ------------------------
 * The Trade-Update transaction is designed to emulate information retrieval and
 * possibly modification by either a customer or a broker to satisfy questions
 * regarding a particular account, a group of trade transaction identifiers or
 * a particular security.
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.4.
 */

/*
 * Frame 1
 * responsible for retrieving information about the specified array of trade
 * IDs and modifying some data from the TRADE table.
 */

CREATE OR REPLACE FUNCTION TradeUpdateFrame1 (
						IN max_trades	integer,
						IN max_updates	integer,
						IN trade_id	bigint[]) RETURNS SETOF record AS $$
DECLARE
	-- output parameters
	num_updated			integer;
	num_found			integer;
	bid_price			numeric(8,2);
	exec_name			varchar;
	is_cash				smallint;
	is_market			smallint;
	trade_price			numeric(8,2);
	settlement_amount		numeric(10,2);
	settlement_cash_due_date	timestamp;
	settlement_cash_type		varchar;
	cash_transaction_amount		numeric(10,2);
	cash_transaction_dts		timestamp;
	cash_transaction_name		varchar;
	trade_history_dts		timestamp[];
	trade_history_status_id		char(4)[];

	-- variables
	exch_name	char(64);
	i		integer;
	j		integer;
	irow_count	integer;
	rs		RECORD;
BEGIN
	num_found = max_trades;
	num_updated = 0;

	i = 1;
	WHILE i <= max_trades LOOP

		-- Get trade information

		IF num_updated < max_updates THEN
			-- Modify the TRADE row for this trade

			SELECT	T_EXEC_NAME
			INTO	exch_name
			FROM 	TRADE
			WHERE	T_ID = trade_id[i];

			IF exch_name like '% X %' THEN
				SELECT	overlay(exch_name placing ' '
				               from position(' X ' in exch_name) for 3)
				INTO 	exch_name;
			ELSE
				SELECT	overlay(exch_name placing ' X '
				                from position(' ' in exch_name) for 3)
				INTO 	exch_name;
			END IF;

			UPDATE	TRADE
			SET	T_EXEC_NAME = exch_name
			WHERE	T_ID = trade_id[i];

			GET DIAGNOSTICS irow_count = ROW_COUNT;
			num_updated = num_updated + irow_count;
		END IF;

		-- will only return one row for each trade

		SELECT	T_BID_PRICE,
			T_EXEC_NAME,
			T_IS_CASH,
			TT_IS_MRKT,
			T_TRADE_PRICE
		INTO	bid_price,
			exec_name,
			is_cash,
			is_market,
			trade_price
		FROM	TRADE,
			TRADE_TYPE
		WHERE	T_ID = trade_id[i] AND
			T_TT_ID = TT_ID;
		
		-- Get settlement information
		-- Will only return one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
		INTO	settlement_amount,
			settlement_cash_due_date,
			settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = trade_id[i];
			
		-- get cash information if this is a cash transaction
		-- Will only return one row for each trade that was a cash transaction

		IF is_cash THEN
			SELECT	CT_AMT,
				CT_DTS,
				CT_NAME
			INTO	cash_transaction_amount,
				cash_transaction_dts,
				cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = trade_id[i];
		END IF;

		-- read trade_history for the trades
		-- Will return 2 to 3 rows per trade

		j = 1;
		FOR rs IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
			WHERE TH_T_ID = trade_id[i] ORDER BY TH_DTS LOOP
			trade_history_dts[j] = rs.TH_DTS;
			trade_history_status_id[j] = rs.TH_ST_ID;
			j = j + 1;
		END LOOP;

		FOR rs IN
				select num_found, num_updated, bid_price, exec_name, is_cash,
				       is_market, trade_price, settlement_amount,
				       extract(year from settlement_cash_due_date),
				       extract(month from settlement_cash_due_date),
				       extract(day from settlement_cash_due_date),
				       extract(hour from settlement_cash_due_date),
				       extract(minute from settlement_cash_due_date),
				       extract(second from settlement_cash_due_date),
				       settlement_cash_type, cash_transaction_amount,
				       extract(year from cash_transaction_dts),
				       extract(month from cash_transaction_dts),
				       extract(day from cash_transaction_dts),
				       extract(hour from cash_transaction_dts),
				       extract(minute from cash_transaction_dts),
				       extract(second from cash_transaction_dts),
				       cash_transaction_name,
				       extract(year from trade_history_dts[1]),
				       extract(month from trade_history_dts[1]), 
				       extract(day from trade_history_dts[1]),
				       extract(hour from trade_history_dts[1]), 
				       extract(minute from trade_history_dts[1]),
				       extract(second from trade_history_dts[1]),
				       trade_history_status_id[1],
				       extract(year from trade_history_dts[2]),
				       extract(month from trade_history_dts[2]),
				       extract(day from trade_history_dts[2]),
				       extract(hour from trade_history_dts[2]),
				       extract(minute from trade_history_dts[2]),
				       extract(second from trade_history_dts[2]),
				       trade_history_status_id[2],
				       extract(year from trade_history_dts[3]),
				       extract(month from trade_history_dts[3]),
				       extract(day from trade_history_dts[3]),
				       extract(hour from trade_history_dts[3]),
				       extract(minute from trade_history_dts[3]),
				       extract(second from trade_history_dts[3]),
				       trade_history_status_id[3]
		LOOP
			RETURN NEXT rs;
		END LOOP;

		i = i + 1;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 2
 * 
 * 
 */

CREATE OR REPLACE FUNCTION TradeUpdateFrame2(
						IN acct_id	IDENT_T,
						IN max_trades	integer,
						IN max_updates	integer,
						IN trade_dts	timestamp)
		RETURNS SETOF record AS $$
DECLARE
	-- output parameters
	settlement_amount		VALUE_T;
	settlement_cash_due_date	timestamp;
	settlement_cash_type		varchar;
	cash_transaction_amount		VALUE_T;
	cash_transaction_dts		timestamp;
	cash_transaction_name		varchar;
	trade_history_dts		timestamp[];
	trade_history_status_id		char(4)[];

	-- variables
	num_updated	integer;
	i		integer;
	j		integer;
	num_found	integer;
	rs		RECORD;
	aux		RECORD;
	cash_type	char(40);
	irow_count	integer;
BEGIN
	-- Get trade information
	-- Should return between 0 and max_trades rows

	i = 0;
	num_updated = 0;
	FOR rs IN SELECT T_BID_PRICE,
			T_EXEC_NAME,
			T_IS_CASH,
			T_ID,
			T_TRADE_PRICE
		FROM	TRADE
		WHERE	T_CA_ID = acct_id AND
			T_DTS >= trade_dts
		ORDER BY T_DTS asc
		LIMIT max_trades LOOP

		IF num_updated < max_updates THEN

			-- Select the SETTLEMENT row for this trade

			SELECT	SE_CASH_TYPE
			INTO	cash_type
			FROM 	SETTLEMENT
			WHERE	SE_T_ID = rs.T_ID;

			IF rs.T_IS_CASH THEN
				IF cash_type = 'Cash Account' THEN
					cash_type = 'Cash';
				ELSE
					cash_type = 'Cash Account';
				END IF;
			ELSE
				IF cash_type = 'Margin Account' THEN
					cash_type = 'Margin';
				ELSE
					cash_type = 'Margin Account';
				END IF;				
			END IF;

			UPDATE	SETTLEMENT
			SET	SE_CASH_TYPE = cash_type
			WHERE	SE_T_ID = rs.T_ID;

			GET DIAGNOSTICS irow_count = ROW_COUNT;
			num_updated = num_updated + irow_count;
		END IF;

		-- Get settlement information
		-- Should return only one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
		INTO	settlement_amount,
			settlement_cash_due_date,
			settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = rs.T_ID;

		-- get cash information if this is a cash transaction
		-- Should return only one row for each trade that was a cash transaction

		IF rs.T_IS_CASH THEN
			SELECT	CT_AMT,
 				CT_DTS,
				CT_NAME
			INTO	cash_transaction_amount,
				cash_transaction_dts,
				cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = rs.T_ID;
		END IF;

		-- read trade_history for the trades
		-- Should return 2 to 3 rows per trade

		j = 1;
		FOR aux IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
			WHERE TH_T_ID = rs.T_ID ORDER BY TH_DTS LOOP
			trade_history_dts[j] = aux.TH_DTS;
			trade_history_status_id[j] = aux.TH_ST_ID;
			j = j + 1;
		END LOOP;

		FOR aux IN
				SELECT num_updated, rs.T_BID_PRICE::S_PRICE_T, rs.T_EXEC_NAME,
				       rs.T_IS_CASH, rs.T_TRADE_PRICE::S_PRICE_T,
				       rs.T_ID::TRADE_T, settlement_amount,
				       extract(year from settlement_cash_due_date),
				       extract(month from settlement_cash_due_date),
				       extract(day from settlement_cash_due_date),
				       extract(hour from settlement_cash_due_date),
				       extract(minute from settlement_cash_due_date),
				       extract(second from settlement_cash_due_date),
				       settlement_cash_type, cash_transaction_amount,
				       extract(year from cash_transaction_dts),
				       extract(month from cash_transaction_dts),
				       extract(day from cash_transaction_dts),
				       extract(hour from cash_transaction_dts),
				       extract(minute from cash_transaction_dts),
				       extract(second from cash_transaction_dts),
				       cash_transaction_name, 
				       extract(year from trade_history_dts[1]),
				       extract(month from trade_history_dts[1]), 
				       extract(day from trade_history_dts[1]),
				       extract(hour from trade_history_dts[1]), 
				       extract(minute from trade_history_dts[1]),
				       extract(second from trade_history_dts[1]),
				       trade_history_status_id[1],
				       extract(year from trade_history_dts[2]),
				       extract(month from trade_history_dts[2]),
				       extract(day from trade_history_dts[2]),
				       extract(hour from trade_history_dts[2]),
				       extract(minute from trade_history_dts[2]),
				       extract(second from trade_history_dts[2]),
				       trade_history_status_id[2],
				       extract(year from trade_history_dts[3]),
				       extract(month from trade_history_dts[3]),
				       extract(day from trade_history_dts[3]),
				       extract(hour from trade_history_dts[3]),
				       extract(minute from trade_history_dts[3]),
				       extract(second from trade_history_dts[3]),
				       trade_history_status_id[3]
		LOOP
			RETURN NEXT aux;
		END LOOP;

		i = i + 1;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 3
 * returns up to N (max_trades) trades for a given security on or after a
 * specified point in time
 * and modifies some data from the CASH_TRANSACTION table.
 */

CREATE OR REPLACE FUNCTION TradeUpdateFrame3(
						IN max_acct_id	IDENT_T,
						IN max_trades	integer,
						IN max_updates	integer,
						IN trade_dts	timestamp,
						IN symbol	char(15)) RETURNS SETOF record AS $$
DECLARE
	-- output parameters
	num_updated			integer;
	settlement_amount		VALUE_T;
	settlement_cash_due_date	timestamp;
	settlement_cash_type		varchar;
	cash_transaction_amount		VALUE_T;
	cash_transaction_dts		timestamp;
	cash_transaction_name		varchar;
	trade_history_dts		timestamp[];
	trade_history_status_id		char(4)[];

	-- Local Frame variables
	i		integer;
	j		integer;
	rs		RECORD;
	aux		RECORD;
	cash_name	char(100);
	irow_count	integer;
BEGIN
	-- Should return between 0 and max_trades rows.

	i = 0;
	num_updated = 0;
	FOR rs IN SELECT T_CA_ID,
			T_EXEC_NAME,
			T_IS_CASH,
			T_ID,
			T_TRADE_PRICE,
			T_QTY,
			T_DTS,
			T_TT_ID,
			TT_NAME,
			S_NAME
		FROM	TRADE,
			TRADE_TYPE,
			SECURITY
		WHERE	T_S_SYMB = symbol AND
			T_DTS >= trade_dts AND
			TT_ID = T_TT_ID AND
			S_SYMB = T_S_SYMB AND
			T_CA_ID <= max_acct_id
		ORDER BY T_DTS asc
		LIMIT max_trades LOOP

		-- Get extra information for each trade in the trade list.
		-- Get settlement information
		-- Will return only one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
		INTO	settlement_amount,
			settlement_cash_due_date,
			settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = rs.T_ID;

		-- get cash information if this is a cash transaction
		-- Will return only one row for each trade that was a cash transaction

		IF rs.T_IS_CASH THEN

			IF num_updated < max_updates THEN
				-- Modify the CASH_TRANSACTION row for this trade
				SELECT	CT_NAME
				INTO	cash_name
				FROM 	CASH_TRANSACTION
				WHERE	CT_T_ID = rs.T_ID;

				IF cash_name like '% shares of %' THEN
					cash_name = rs.TT_NAME || ' ' || rs.T_QTY || ' Shares of ' || rs.S_NAME;
				ELSE
					cash_name = rs.TT_NAME || ' ' || rs.T_QTY || ' shares of ' || rs.S_NAME;
				END IF;

				UPDATE	CASH_TRANSACTION
				SET	CT_NAME = cash_name
				WHERE	CT_T_ID = rs.T_ID;

				GET DIAGNOSTICS irow_count = ROW_COUNT;

				num_updated = num_updated + irow_count;

			END IF;

			SELECT	CT_AMT,
 				CT_DTS,
				CT_NAME
			INTO	cash_transaction_amount,
				cash_transaction_dts,
				cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = rs.T_ID;
		END IF;

		-- read trade_history for the trades
		-- Should return 2 to 3 rows per trade

		j = 1;
		FOR aux IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
			WHERE TH_T_ID = rs.T_ID ORDER BY TH_DTS LOOP
			trade_history_dts[j] = aux.TH_DTS;
			trade_history_status_id[j] = aux.TH_ST_ID;
			j = j + 1;
		END LOOP;

		FOR aux IN
				SELECT 0, rs.T_CA_ID, cash_transaction_amount,
				       extract(year from cash_transaction_dts),
				       extract(month from cash_transaction_dts),
				       extract(day from cash_transaction_dts),
				       extract(hour from cash_transaction_dts),
				       extract(minute from cash_transaction_dts),
				       extract(second from cash_transaction_dts),
				       cash_transaction_name, rs.T_EXEC_NAME,
				       rs.T_IS_CASH, rs.T_TRADE_PRICE, rs.T_QTY,
				       settlement_amount, 
				       extract(year from settlement_cash_due_date),
				       extract(month from settlement_cash_due_date),
				       extract(day from settlement_cash_due_date),
				       extract(hour from settlement_cash_due_date),
				       extract(minute from settlement_cash_due_date),
				       extract(second from settlement_cash_due_date),
				       settlement_cash_type, extract(year from rs.T_DTS),
				       extract(month from rs.T_DTS),
				       extract(day from rs.T_DTS), extract(hour from rs.T_DTS),
				       extract(minute from rs.T_DTS), 
				       extract(second from rs.T_DTS),
				       extract(year from trade_history_dts[1]), 
				       extract(month from trade_history_dts[1]),
				       extract(day from trade_history_dts[1]), 
				       extract(hour from trade_history_dts[1]),
				       extract(minute from trade_history_dts[1]), 
				       extract(second from trade_history_dts[1]),
				       trade_history_status_id[1],
				       extract(year from trade_history_dts[2]),
				       extract(month from trade_history_dts[2]),
				       extract(day from trade_history_dts[2]), 
				       extract(hour from trade_history_dts[2]),
				       extract(minute from trade_history_dts[2]), 
				       extract(second from trade_history_dts[2]),
				       trade_history_status_id[2],
				       extract(year from trade_history_dts[3]),
				       extract(month from trade_history_dts[3]),
				       extract(day from trade_history_dts[3]),
				       extract(hour from trade_history_dts[3]),
				       extract(minute from trade_history_dts[3]),
				       extract(second from trade_history_dts[3]),
				       trade_history_status_id[3], rs.T_ID, rs.T_TT_ID
		LOOP
			RETURN NEXT aux;
		END LOOP;

		i = i + 1;
	END LOOP;

	-- send num_updated
	FOR aux IN
			SELECT num_updated, rs.T_CA_ID, cash_transaction_amount,
			       extract(year from cash_transaction_dts),
			       extract(month from cash_transaction_dts),
			       extract(day from cash_transaction_dts),
			       extract(hour from cash_transaction_dts),
			       extract(minute from cash_transaction_dts),
			       extract(second from cash_transaction_dts),
			       cash_transaction_name, rs.T_EXEC_NAME,
			       rs.T_IS_CASH, rs.T_TRADE_PRICE, rs.T_QTY, settlement_amount, 
			       extract(year from settlement_cash_due_date),
			       extract(month from settlement_cash_due_date),
			       extract(day from settlement_cash_due_date),
			       extract(hour from settlement_cash_due_date),
			       extract(minute from settlement_cash_due_date),
			       extract(second from settlement_cash_due_date),
			       settlement_cash_type, extract(year from rs.T_DTS),
			       extract(month from rs.T_DTS),
			       extract(day from rs.T_DTS), extract(hour from rs.T_DTS),
			       extract(minute from rs.T_DTS), 
			       extract(second from rs.T_DTS),
			       extract(year from trade_history_dts[1]), 
			       extract(month from trade_history_dts[1]),
			       extract(day from trade_history_dts[1]), 
			       extract(hour from trade_history_dts[1]),
			       extract(minute from trade_history_dts[1]), 
			       extract(second from trade_history_dts[1]),
			       trade_history_status_id[1],
			       extract(year from trade_history_dts[2]),
			       extract(month from trade_history_dts[2]),
			       extract(day from trade_history_dts[2]), 
			       extract(hour from trade_history_dts[2]),
			       extract(minute from trade_history_dts[2]), 
			       extract(second from trade_history_dts[2]),
			       trade_history_status_id[2],
			       extract(year from trade_history_dts[3]),
			       extract(month from trade_history_dts[3]),
			       extract(day from trade_history_dts[3]),
			       extract(hour from trade_history_dts[3]),
			       extract(minute from trade_history_dts[3]),
			       extract(second from trade_history_dts[3]),
			       trade_history_status_id[3], rs.T_ID, rs.T_TT_ID
	LOOP
		RETURN NEXT aux;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
