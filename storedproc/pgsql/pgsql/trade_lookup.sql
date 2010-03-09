/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Trade Lookup transaction
 * ------------------------
 * The Trade-Lookup transaction is designed to emulate the information
 * retrieval by 
 * either a customer or a broker to satisfy their questions regarding a
 * particular account, a group of trade transaction identifiers or a 
 * particular security
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.3.
 */

/*
 * Frame 1
 * responsible for retrieving information about the specified array of trade
 * IDs.
 */

CREATE OR REPLACE FUNCTION TradeLookupFrame1 (
						IN max_trades integer,
						IN trade_id bigint[]) RETURNS SETOF record AS $$
DECLARE
	-- output parameters
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
	i		integer;
	j		integer;
	num_found	integer;
	rs		RECORD;
BEGIN
	num_found = max_trades;

	i = 1;
	WHILE i <= max_trades LOOP

		-- Get trade information
		-- Should only return one row for each trade

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
		-- Should only return one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
		INTO	settlement_amount,
			settlement_cash_due_date,
			settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = trade_id[i];
			
		-- get cash information if this is a cash transaction
		-- Should only return one row for each trade that was a cash transaction

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
		-- Should return 2 to 3 rows per trade

		j = 1;
		FOR rs IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
			WHERE TH_T_ID = trade_id[i] ORDER BY TH_DTS LOOP
			trade_history_dts[j] = rs.TH_DTS;
			trade_history_status_id[j] = rs.TH_ST_ID;
			j = j + 1;
		END LOOP;

		FOR rs IN 
				select bid_price, exec_name, is_cash, is_market, trade_price,
				       settlement_amount,
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
 * returns information for the first N (max_trades) trades executed for the
 * specified customer account at or after the specified time.
 */

CREATE OR REPLACE FUNCTION TradeLookupFrame2(
						IN acct_id	IDENT_T,
						IN max_trades	integer,
						IN trade_dts	timestamp) RETURNS SETOF record AS $$
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
	i		integer;
	j		integer;
	num_found	integer;
	rs		RECORD;
	aux		RECORD;
BEGIN
	-- Get trade information
	-- Should return between 0 and max_trades rows

	i = 0;
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
				SELECT rs.T_BID_PRICE::S_PRICE_T, rs.T_EXEC_NAME, rs.T_IS_CASH,
				       rs.T_TRADE_PRICE::S_PRICE_T, rs.T_ID::TRADE_T,
				       settlement_amount,
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
 * specified point in time.
 */

CREATE OR REPLACE FUNCTION TradeLookupFrame3(
						IN max_acct_id	IDENT_T,
						IN max_trades	integer,
						IN trade_dts	timestamp,
						IN symbol	char(15)) RETURNS SETOF record AS $$
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

	-- Local Frame variables
	i	integer;
	j	integer;
	rs	RECORD;
	aux	RECORD;
BEGIN
	-- Should return between 0 and max_trades rows.

	i = 0;
	FOR rs IN SELECT T_CA_ID,
			T_EXEC_NAME,
			T_IS_CASH,
			T_ID,
			T_TRADE_PRICE,
			T_QTY,
			T_DTS,
			T_TT_ID
		FROM	TRADE
		WHERE	T_S_SYMB = symbol AND
			T_DTS >= trade_dts AND
			T_CA_ID <= max_acct_id
		ORDER BY T_DTS asc
		LIMIT max_trades LOOP

		-- Get extra information for each trade in the trade list.
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
				SELECT rs.T_CA_ID, cash_transaction_amount,
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
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 4
 * identifies the first trade for the specified customer account on or after
 * the specified time.
 */

CREATE OR REPLACE FUNCTION TradeLookupFrame4(
		IN acct_id	IDENT_T,
		IN trade_dts	timestamp) RETURNS SETOF record AS $$
DECLARE
	-- Local Frame variables
	rs		RECORD;
	-- output parameters	
	trade_id	TRADE_T;
BEGIN
	SELECT	T_ID
	INTO	trade_id
	FROM	TRADE
	WHERE	T_CA_ID = acct_id AND
		T_DTS >= trade_dts
	ORDER BY T_DTS asc
	LIMIT 1;

	-- The trade_id is used in the subquery to find the original trade_id
	-- (HH_H_T_ID), which then is used to list all the entries.
	-- Should return 0 to 20 rows.
	
	FOR rs IN SELECT HH_H_T_ID,
			HH_T_ID,
			HH_BEFORE_QTY,
			HH_AFTER_QTY,
			trade_id
			FROM	HOLDING_HISTORY
			WHERE	HH_H_T_ID in
				(SELECT	HH_H_T_ID
				FROM	HOLDING_HISTORY
				WHERE	HH_T_ID = trade_id)
			LIMIT 20 
	LOOP
		RETURN NEXT rs;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
