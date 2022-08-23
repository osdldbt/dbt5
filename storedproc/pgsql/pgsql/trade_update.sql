/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0
 */

-- Clause 3.3.10.3
CREATE OR REPLACE FUNCTION TradeUpdateFrame1 (
						IN max_trades	integer,
						IN max_updates	integer,
    IN trade_id	IDENT_T[20]
  , OUT bid_price S_PRICE_T[20]
  , OUT cash_transaction_amount VALUE_T[20]
  , OUT cash_transaction_dts TIMESTAMP[20]
  , OUT cash_transaction_name VARCHAR(100)[20]
  , OUT exec_name VARCHAR(64)[20]
  , OUT is_cash SMALLINT[20]
  , OUT is_market SMALLINT[20]
  , OUT num_found INTEGER
  , OUT num_updated INTEGER
  , OUT settlement_amount VALUE_T[20]
  , OUT settlement_cash_due_date TIMESTAMP[20]
  , OUT settlement_cash_type VARCHAR(40)[20]
  , OUT trade_history_dts TIMESTAMP[20][3]
  , OUT trade_history_status_id VARCHAR(4)[20][3]
  , OUT trade_price S_PRICE_T[20]
) RETURNS RECORD AS $$
DECLARE
	-- variables
	exch_name	char(64);
	i		integer;
	j		integer;
    k INTEGER;
	irow_count	integer;
	rs		RECORD;

    tmp_bid_price S_PRICE_T;
    tmp_exec_name VARCHAR(64);
    tmp_is_cash BOOLEAN;
    tmp_is_market BOOLEAN;
    tmp_trade_price S_PRICE_T;

    tmp_settlement_amount VALUE_T;
    tmp_settlement_cash_due_date TIMESTAMP;
    tmp_settlement_cash_type VARCHAR(40);

    tmp_cash_transaction_amount VALUE_T;
    tmp_cash_transaction_dts TIMESTAMP;
    tmp_cash_transaction_name VARCHAR(100);
BEGIN
    num_found = 0;
	num_updated = 0;

    i = 0;
    WHILE i < max_trades LOOP
        i = i + 1;

		-- Get trade information

		IF num_updated < max_updates THEN
			-- Modify the TRADE row for this trade

			SELECT	T_EXEC_NAME
			INTO	exch_name
			FROM 	TRADE
			WHERE	T_ID = trade_id[i];

            GET DIAGNOSTICS irow_count = ROW_COUNT;
            num_found := num_found + irow_count;

			IF exch_name like '% X %' THEN
                SELECT replace(exch_name, ' ', ' X ')
				INTO 	exch_name;
			ELSE
                SELECT replace(exch_name, ' X ', ' ')
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
        INTO tmp_bid_price
          , tmp_exec_name
          , tmp_is_cash
          , tmp_is_market
          , tmp_trade_price
		FROM	TRADE,
			TRADE_TYPE
		WHERE	T_ID = trade_id[i] AND
			T_TT_ID = TT_ID;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            bid_price[i] := tmp_bid_price;
            exec_name[i] := tmp_exec_name;
            IF tmp_is_cash THEN
                is_cash[i] := 1;
            ELSE
                is_cash[i] := 0;
            END IF;
            IF tmp_is_market THEN
                is_market[i] := 1;
            ELSE
                is_market[i] := 0;
            END IF;
            trade_price[i] := tmp_trade_price;
        END IF;
		
		-- Get settlement information
		-- Will only return one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
        INTO tmp_settlement_amount
          , tmp_settlement_cash_due_date
          , tmp_settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = trade_id[i];

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            settlement_amount[i] := tmp_settlement_amount;
            settlement_cash_due_date[i] := tmp_settlement_cash_due_date;
            settlement_cash_type[i] := tmp_settlement_cash_type;
        END IF;

		-- get cash information if this is a cash transaction
		-- Will only return one row for each trade that was a cash transaction

        IF tmp_is_cash THEN
			SELECT	CT_AMT,
				CT_DTS,
				CT_NAME
            INTO tmp_cash_transaction_amount
              , tmp_cash_transaction_dts
              , tmp_cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = trade_id[i];
		END IF;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            cash_transaction_amount[i] := tmp_cash_transaction_amount;
            cash_transaction_dts[i] := tmp_cash_transaction_dts;
            cash_transaction_name[i] := tmp_cash_transaction_name;
        END IF;

		-- read trade_history for the trades
		-- Will return 2 to 3 rows per trade

        j := 0;
        k := (i - 1) * 3 + 1;
        trade_history_dts[k + j] := NULL;
        trade_history_dts[k + j + 1] := NULL;
        trade_history_dts[k + j + 2] := NULL;
        trade_history_status_id[k + j] := NULL;
        trade_history_status_id[k + j + 1] := NULL;
        trade_history_status_id[k + j + 2] := NULL;
		FOR rs IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
            WHERE TH_T_ID = trade_id[i]
            ORDER BY TH_DTS
            LIMIT 3
        LOOP
            trade_history_dts[k + j] = rs.th_dts;
            trade_history_status_id[k + j] = rs.th_st_id;
            j = j + 1;
		END LOOP;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';

-- Clause 3.3.10.4
CREATE OR REPLACE FUNCTION TradeUpdateFrame2(
						IN acct_id	IDENT_T,
    IN end_trade_dts TIMESTAMP,
						IN max_trades	integer,
						IN max_updates	integer,
    IN start_trade_dts TIMESTAMP
  , OUT bid_price VALUE_T[20]
  , OUT cash_transaction_amount VALUE_T[20]
  , OUT cash_transaction_dts TIMESTAMP[20]
  , OUT cash_transaction_name VARCHAR[20]
  , OUT exec_name VARCHAR(64)[20]
  , OUT is_cash SMALLINT[20]
  , OUT num_found INTEGER
  , OUT num_updated INTEGER
  , OUT settlement_amount VALUE_T[20]
  , OUT settlement_cash_due_date TIMESTAMP[20]
  , OUT settlement_cash_type VARCHAR(40)[20]
  , OUT trade_history_dts TIMESTAMP[20][3]
  , OUT trade_history_status_id VARCHAR(4)[20][3]
  , OUT trade_list IDENT_T[20]
  , OUT trade_price VALUE_T[20]
) RETURNS RECORD AS $$
DECLARE
	-- variables
	i		integer;
	j		integer;
    k INTEGER;
	rs		RECORD;
	aux		RECORD;
	cash_type	char(40);
	irow_count	integer;

    tmp_settlement_amount VALUE_T;
    tmp_settlement_cash_due_date TIMESTAMP;
    tmp_settlement_cash_type VARCHAR(40);

    tmp_cash_transaction_amount VALUE_T;
    tmp_cash_transaction_dts TIMESTAMP;
    tmp_cash_transaction_name VARCHAR(100);
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
              t_dts >= start_trade_dts
          AND t_dts <= end_trade_dts
		ORDER BY t_dts ASC LOOP
        i = i + 1;

        bid_price[i] := rs.t_bid_price;
        exec_name[i] := rs.t_exec_name;
        IF rs.t_is_cash THEN
            is_cash[i] := 1;
        ELSE
            is_cash[i] := 0;
        END IF;
        trade_list[i] := rs.t_id;
        trade_price[i] := rs.t_trade_price;

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
        INTO tmp_settlement_amount
          , tmp_settlement_cash_due_date
          , tmp_settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = rs.T_ID;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            settlement_amount[i] := tmp_settlement_amount;
            settlement_cash_due_date[i] := tmp_settlement_cash_due_date;
            settlement_cash_type[i] := tmp_settlement_cash_type;
        END IF;

		-- get cash information if this is a cash transaction
		-- Should return only one row for each trade that was a cash transaction

		IF rs.T_IS_CASH THEN
			SELECT	CT_AMT,
 				CT_DTS,
				CT_NAME
            INTO tmp_cash_transaction_amount
              , tmp_cash_transaction_dts
              , tmp_cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = rs.T_ID;
		END IF;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            cash_transaction_amount[i] := tmp_cash_transaction_amount;
            cash_transaction_dts[i] := tmp_cash_transaction_dts;
            cash_transaction_name[i] := tmp_cash_transaction_name;
        END IF;

		-- read trade_history for the trades
		-- Should return 2 to 3 rows per trade

        j := 0;
        k := (i - 1) * 3 + 1;
        trade_history_dts[k + j] := NULL;
        trade_history_dts[k + j + 1] := NULL;
        trade_history_dts[k + j + 2] := NULL;
        trade_history_status_id[k + j] := NULL;
        trade_history_status_id[k + j + 1] := NULL;
        trade_history_status_id[k + j + 2] := NULL;
		FOR aux IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
            WHERE th_t_id = rs.t_id
            ORDER BY th_dts
            LIMIT 3
        LOOP
            trade_history_dts[k + j] = aux.th_dts;
            trade_history_status_id[k + j] = aux.th_st_id;
            j = j + 1;
		END LOOP;

        IF i >= max_trades THEN
            EXIT;
        END IF;
	END LOOP;
    num_found := i;
END;
$$ LANGUAGE 'plpgsql';

-- Clause 3.3.10.5
CREATE OR REPLACE FUNCTION TradeUpdateFrame3(
    IN end_trade_dts TIMESTAMP,
						IN max_acct_id	IDENT_T,
						IN max_trades	integer,
						IN max_updates	integer,
    IN start_trade_dts TIMESTAMP
  , IN symbol CHAR(15)
  , OUT acct_id IDENT_T[20]
  , OUT cash_transaction_amount VALUE_T[20]
  , OUT cash_transaction_dts TIMESTAMP[20]
  , OUT cash_transaction_name VARCHAR[20]
  , OUT exec_name VARCHAR(64)[20]
  , OUT is_cash SMALLINT[20]
  , OUT num_found INTEGER
  , OUT num_updated INTEGER
  , OUT price VALUE_T[20]
  , OUT quantity INTEGER[20]
  , OUT s_name VARCHAR(70)[20]
  , OUT settlement_amount VALUE_T[20]
  , OUT settlement_cash_due_date TIMESTAMP[20]
  , OUT settlement_cash_type VARCHAR(40)[20]
  , OUT trade_dts TIMESTAMP[20]
  , OUT trade_history_dts TIMESTAMP[20][3]
  , OUT trade_history_status_id VARCHAR(4)[20][3]
  , OUT trade_list IDENT_T[20]
  , OUT type_name VARCHAR(12)[20]
  , OUT trade_type VARCHAR(12)[20]
) RETURNS RECORD AS $$
<<tuf3>>
DECLARE
	-- Local Frame variables
	i		integer;
	j		integer;
    k INTEGER;
	rs		RECORD;
	aux		RECORD;
    ct_name	CHAR(100);
	irow_count	integer;

    tmp_settlement_amount VALUE_T;
    tmp_settlement_cash_due_date TIMESTAMP;
    tmp_settlement_cash_type VARCHAR(40);

    tmp_cash_transaction_amount VALUE_T;
    tmp_cash_transaction_dts TIMESTAMP;
    tmp_cash_transaction_name VARCHAR(100);
BEGIN
	-- Should return between 0 and max_trades rows.

	i = 0;
	FOR rs IN SELECT T_CA_ID,
			T_EXEC_NAME,
			T_IS_CASH,
			T_TRADE_PRICE,
			T_QTY,
            security.s_name,
			T_DTS,
            t_id,
			T_TT_ID,
            tt_name
		FROM	TRADE,
			TRADE_TYPE,
			SECURITY
		WHERE	T_S_SYMB = symbol AND
            t_dts >= start_trade_dts
          AND t_dts <= end_trade_dts AND
			TT_ID = T_TT_ID AND
              s_symb = t_s_symb
		ORDER BY T_DTS asc
		LIMIT max_trades LOOP
        i = i + 1;

        acct_id[i] := rs.t_ca_id;
        exec_name[i] := rs.t_exec_name;
        IF rs.t_is_cash THEN
            is_cash[i] := 1;
        ELSE
            is_cash[i] := 0;
        END IF;
        price[i] := rs.t_trade_price;
        quantity[i] := rs.t_qty;
        s_name[i] := rs.s_name;
        trade_dts[i] := rs.t_dts;
        trade_list[i] := rs.t_id;
        trade_type[i] := rs.t_tt_id;
        type_name[i] := rs.tt_name;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        num_found := irow_count;
        num_updated = 0;

		-- Get extra information for each trade in the trade list.
		-- Get settlement information
		-- Will return only one row for each trade

		SELECT	SE_AMT,
			SE_CASH_DUE_DATE,
			SE_CASH_TYPE
        INTO tmp_settlement_amount
          , tmp_settlement_cash_due_date
          , tmp_settlement_cash_type
		FROM	SETTLEMENT
		WHERE	SE_T_ID = rs.T_ID;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            settlement_amount[i] := tmp_settlement_amount;
            settlement_cash_due_date[i] := tmp_settlement_cash_due_date;
            settlement_cash_type[i] := tmp_settlement_cash_type;
        END IF;

		-- get cash information if this is a cash transaction
		-- Will return only one row for each trade that was a cash transaction

		IF rs.T_IS_CASH THEN

			IF num_updated < max_updates THEN
				-- Modify the CASH_TRANSACTION row for this trade
                SELECT	cash_transaction.ct_name
                INTO ct_name
				FROM 	CASH_TRANSACTION
				WHERE	CT_T_ID = rs.T_ID;

                IF ct_name like '% shares of %' THEN
                    ct_name = rs.TT_NAME || ' ' || rs.T_QTY || ' Shares of ' || rs.S_NAME;
				ELSE
                    ct_name = rs.TT_NAME || ' ' || rs.T_QTY || ' shares of ' || rs.S_NAME;
				END IF;

				UPDATE	CASH_TRANSACTION
                SET	ct_name = tuf3.ct_name
				WHERE	CT_T_ID = rs.T_ID;

				GET DIAGNOSTICS irow_count = ROW_COUNT;

				num_updated = num_updated + irow_count;

			END IF;

			SELECT	CT_AMT,
 				CT_DTS,
                   cash_transaction.ct_name
            INTO tmp_cash_transaction_amount
              , tmp_cash_transaction_dts
              , tmp_cash_transaction_name
			FROM	CASH_TRANSACTION
			WHERE	CT_T_ID = rs.T_ID;
		END IF;

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        IF irow_count > 0 THEN
            cash_transaction_amount[i] := tmp_cash_transaction_amount;
            cash_transaction_dts[i] := tmp_cash_transaction_dts;
            cash_transaction_name[i] := tmp_cash_transaction_name;
        END IF;

		-- read trade_history for the trades
		-- Should return 2 to 3 rows per trade

        j := 0;
        k := (i - 1) * 3 + 1;
        trade_history_dts[k + j] := NULL;
        trade_history_dts[k + j + 1] := NULL;
        trade_history_dts[k + j + 2] := NULL;
        trade_history_status_id[k + j] := NULL;
        trade_history_status_id[k + j + 1] := NULL;
        trade_history_status_id[k + j + 2] := NULL;
		FOR aux IN SELECT TH_DTS, TH_ST_ID 
			FROM TRADE_HISTORY
			WHERE TH_T_ID = rs.T_ID ORDER BY TH_DTS LOOP
            trade_history_dts[k + j] = aux.th_dts;
            trade_history_status_id[k + j] = aux.th_st_id;
            j = j + 1;
        END LOOP;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
