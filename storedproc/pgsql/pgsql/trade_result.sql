/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Trade Result transaction
 * ------------------------
 * The Trade-Result transaction represents the completion of a stock market
 * trade.
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.2.
 */

/*
 * Frame 1
 * responsible for retrieving information about the customer and its trade.
 */

CREATE OR REPLACE FUNCTION TradeResultFrame1 (IN trade_id TRADE_T)
		RETURNS record AS $$
DECLARE
	-- output parameters
	acct_id		IDENT_T;
	charge		VALUE_T;
	holdsum_qty	S_QTY_T;
	is_lifo		smallint;
	symbol		char(15);
	trade_is_cash	smallint;
	trade_qty	S_QTY_T;
	type_id		char(3);
	type_is_market	smallint;	
	type_is_sell	smallint;
	type_name	char(12);

	-- variables
	rs RECORD;
BEGIN
	SELECT  T_CA_ID,
		T_TT_ID,
		T_S_SYMB,
		T_QTY,
		T_CHRG,
		T_LIFO,
		T_IS_CASH
	INTO	acct_id,
		type_id,
		symbol,
		trade_qty,
		charge,
		is_lifo,
		trade_is_cash
	FROM	TRADE
	WHERE	T_ID = trade_id;

	SELECT	TT_NAME,
		TT_IS_SELL,
		TT_IS_MRKT
	INTO	type_name,
		type_is_sell,
		type_is_market
	FROM	TRADE_TYPE
	WHERE	TT_ID = type_id;

	SELECT	HS_QTY
	INTO	holdsum_qty
	FROM	HOLDING_SUMMARY
	WHERE	HS_CA_ID = acct_id AND
		HS_S_SYMB = symbol;

	IF holdsum_qty is NULL THEN -- no prior holdings exist
		holdsum_qty = 0;
	END IF;

	-- return data
	SELECT	acct_id,
		charge,
		holdsum_qty,
		is_lifo,
		symbol,
		trade_is_cash,
		trade_qty,
		type_id,
		type_is_market,
		type_is_sell,
		type_name
	INTO	rs;

	RETURN	rs;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 2
 * responsible for modifying the customer's holdings to reflect the result
 * of a buy or a sell trade.
 */

CREATE OR REPLACE FUNCTION TradeResultFrame2(
				IN acct_id	IDENT_T,
				IN holdsum_qty	S_QTY_T,
				IN is_lifo	smallint,
				IN symbol	char(15),
				IN trade_id	TRADE_T,
				IN trade_price	S_PRICE_T,
				IN trade_qty	S_QTY_T,
				IN type_is_sell	smallint) RETURNS record AS $$
DECLARE
	-- output parameters
	broker_id	IDENT_T;
	buy_value	numeric(12,2);
	cust_id		IDENT_T;
	sell_value	numeric(12,2);
	tax_status	smallint;
	trade_dts	timestamp;

	-- variables
	hold_id		IDENT_T;
	hold_price	S_PRICE_T;
	hold_qty	S_QTY_T;
	needed_qty	S_QTY_T;
	rs		RECORD;

	-- cursor
	hold_list	refcursor;
BEGIN
	-- Get the timestamp
	SELECT	NOW()
	INTO	trade_dts;

	-- Initialize variables
	buy_value = 0.0;
	sell_value = 0.0;
	needed_qty = trade_qty;

	SELECT	CA_B_ID,
		CA_C_ID,
		CA_TAX_ST
	INTO	broker_id,
		cust_id,
		tax_status
	FROM	CUSTOMER_ACCOUNT
	WHERE	CA_ID = acct_id;

	-- Determine if sell or buy order
	IF type_is_sell THEN 

		IF holdsum_qty = 0 THEN
			-- no prior holdings exist, but one will be inserted
			INSERT INTO	HOLDING_SUMMARY (
						HS_CA_ID,
						HS_S_SYMB,
						HS_QTY)
			VALUES 		(acct_id, symbol, (-1)*trade_qty);
		ELSE
			IF holdsum_qty != trade_qty THEN
				UPDATE	HOLDING_SUMMARY
				SET	HS_QTY = (holdsum_qty::integer - trade_qty::integer)
				WHERE	HS_CA_ID = acct_id AND
					HS_S_SYMB = symbol;
			END IF;
		END IF;

		-- Sell Trade:
		-- First look for existing holdings, H_QTY > 0
		IF holdsum_qty > 0 THEN

			IF is_lifo THEN
				-- Could return 0, 1 or many rows
				OPEN 	hold_list FOR
				SELECT	H_T_ID,
					H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
					H_S_SYMB = symbol
				ORDER BY H_DTS desc;
			ELSE
				-- Could return 0, 1 or many rows
				OPEN	hold_list FOR
				SELECT	H_T_ID,
					H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
					H_S_SYMB = symbol
				ORDER BY H_DTS asc;
			END IF;

			-- Liquidate existing holdings. Note that more than
			-- 1 HOLDING record can be deleted here since customer
			-- may have the same security with differing prices.
			
			WHILE needed_qty > 0 LOOP
				FETCH	hold_list
				INTO	hold_id,
					hold_qty,
					hold_price;
				EXIT WHEN NOT FOUND;

				IF hold_qty > needed_qty THEN
					-- Selling some of the holdings
					INSERT INTO	HOLDING_HISTORY (
								HH_H_T_ID,
								HH_T_ID,
								HH_BEFORE_QTY,
								HH_AFTER_QTY)
					VALUES (		hold_id, -- H_T_ID of original trade
								trade_id, -- T_ID current trade
								hold_qty, -- H_QTY now
								(hold_qty - needed_qty)); -- H_QTY after update
					
					UPDATE	HOLDING
					SET	H_QTY = (hold_qty - needed_qty)
					WHERE	H_T_ID = hold_id; -- current of hold_list;

					buy_value = buy_value + (needed_qty * hold_price);
					sell_value = sell_value + (needed_qty * trade_price);
					needed_qty = 0;
				ELSE
					-- Selling all holdings
					INSERT INTO	HOLDING_HISTORY (
								HH_H_T_ID,
								HH_T_ID,
								HH_BEFORE_QTY,
								HH_AFTER_QTY)
					VALUES (		hold_id, -- H_T_ID original trade
								trade_id, -- T_ID current trade
								hold_qty, -- H_QTY now
								0); -- H_QTY after delete
					
					DELETE FROM	HOLDING
					WHERE		H_T_ID = hold_id; -- current of hold_list;

					buy_value = buy_value + (hold_qty * hold_price);
					sell_value = sell_value + (hold_qty * trade_price);
					needed_qty = needed_qty - hold_qty;
				END IF;
			END LOOP;

			CLOSE	hold_list;
		END IF;

		-- Sell Short:
		-- If needed_qty > 0 then customer has sold all existing
		-- holdings and customer is selling short. A new HOLDING
		-- record will be created with H_QTY set to the negative
		-- number of needed shares.

		IF needed_qty > 0 THEN
			INSERT INTO	HOLDING_HISTORY (
						HH_H_T_ID,
						HH_T_ID,
						HH_BEFORE_QTY,
						HH_AFTER_QTY)
			VALUES (		trade_id, -- T_ID current is original trade
						trade_id, -- T_ID current trade
						0, -- H_QTY before
						(-1) * needed_qty); -- H_QTY after insert
			
			INSERT INTO	HOLDING (
						H_T_ID,
						H_CA_ID,
						H_S_SYMB,
						H_DTS,
						H_PRICE,
						H_QTY)
			VALUES (		trade_id, -- H_T_ID
						acct_id, -- H_CA_ID
						symbol, -- H_S_SYMB
						trade_dts, -- H_DTS
						trade_price, -- H_PRICE
						(-1) * needed_qty); -- * H_QTY
		ELSE
			IF holdsum_qty = trade_qty THEN
				DELETE FROM	HOLDING_SUMMARY
				WHERE		HS_CA_ID = acct_id AND
						HS_S_SYMB = symbol;
			END IF;
		END IF;

	ELSE -- The trade is a BUY

		IF holdsum_qty = 0 THEN
			-- no prior holdings exist, but one will be inserted
			INSERT INTO	HOLDING_SUMMARY (
						HS_CA_ID,
						HS_S_SYMB,
						HS_QTY)
			VALUES (		acct_id,
						symbol,
						trade_qty);
		ELSE -- holdsum_qty != 0
			IF -holdsum_qty != trade_qty THEN
				UPDATE	HOLDING_SUMMARY
				SET	HS_QTY = holdsum_qty + trade_qty
				WHERE	HS_CA_ID = acct_id AND
					HS_S_SYMB = symbol;
			END IF;
		END IF;

		-- Short Cover:
		-- First look for existing negative holdings, H_QTY < 0,
		-- which indicates a previous short sell. The buy trade
		-- will cover the short sell.

		IF holdsum_qty < 0 THEN
			IF is_lifo THEN
				-- Could return 0, 1 or many rows
				OPEN 	hold_list FOR
				SELECT	H_T_ID,
					H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
					H_S_SYMB = symbol
				ORDER BY H_DTS desc;
			ELSE
				-- Could return 0, 1 or many rows
				OPEN 	hold_list FOR
				SELECT	H_T_ID,
					H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
					H_S_SYMB = symbol
				ORDER BY H_DTS asc;
			END IF;

			-- Buy back securities to cover a short position.
			
			WHILE needed_qty > 0 LOOP
				FETCH	hold_list
				INTO	hold_id,
					hold_qty,
					hold_price;
				EXIT WHEN NOT FOUND;

				IF (hold_qty + needed_qty < 0) THEN
					-- Buying back some of the Short Sell
					INSERT INTO	HOLDING_HISTORY (
								HH_H_T_ID,
								HH_T_ID,
								HH_BEFORE_QTY,
								HH_AFTER_QTY)
					VALUES (		hold_id, -- H_T_ID original trade
								trade_id, -- T_ID current trade
								hold_qty, -- H_QTY now
								(hold_qty + needed_qty)); -- H_QTY after update
					
					UPDATE	HOLDING
					SET	H_QTY = (hold_qty + needed_qty)
					WHERE	H_T_ID = hold_id;	--current of hold_list;
	
					sell_value = sell_value + (needed_qty * hold_price);
					buy_value = buy_value + (needed_qty * trade_price);
					needed_qty = 0;
				ELSE
					-- Buying back all of the Short Sell
					INSERT INTO	HOLDING_HISTORY (
								HH_H_T_ID,
								HH_T_ID,
								HH_BEFORE_QTY,
								HH_AFTER_QTY)
					VALUES (		hold_id, -- H_T_ID original trade
								trade_id, -- T_ID current trade
								hold_qty, -- H_QTY now
								0); -- H_QTY after delete
					
					DELETE FROM	HOLDING
					WHERE		H_T_ID = hold_id;	--current of hold_list;
	
					-- Make hold_qty positive for easy calculations
					hold_qty = -hold_qty;
					sell_value = sell_value + (hold_qty * hold_price);
					buy_value = buy_value + (hold_qty * trade_price);
					needed_qty = needed_qty - hold_qty;
				END IF;
			END LOOP;
			CLOSE	hold_list;
		END IF;

		-- Buy Trade:
		-- If needed_qty > 0, then the customer has covered all
		-- previous Short Sells and the customer is buying new
		-- holdings. A new HOLDING record will be created with
		-- H_QTY set to the number of needed shares.
	
		IF needed_qty > 0 THEN
			INSERT INTO	HOLDING_HISTORY (
						HH_H_T_ID,
						HH_T_ID,
						HH_BEFORE_QTY,
						HH_AFTER_QTY)
			VALUES (		trade_id, -- T_ID current is original trade
						trade_id, -- * T_ID current trade
						0, -- H_QTY before
						needed_qty); -- H_QTY after insert
			
			INSERT INTO	HOLDING (
						H_T_ID, 
						H_CA_ID,
						H_S_SYMB,
						H_DTS,
						H_PRICE,
						H_QTY)
			VALUES (		trade_id, -- H_T_ID
						acct_id, -- H_CA_ID
						symbol, -- H_S_SYMB
						trade_dts, -- H_DTS
						trade_price, -- H_PRICE
						needed_qty); -- H_QTY
		ELSE
			IF (-holdsum_qty = trade_qty) THEN
				DELETE FROM	HOLDING_SUMMARY
				WHERE		HS_CA_ID = acct_id AND
						HS_S_SYMB = symbol;
			END IF;
		END IF;

	END IF;

	-- Return output parameters
	SELECT	broker_id,
		buy_value,
		cust_id,
		sell_value,
		tax_status,
		extract(year from trade_dts),
		extract(month from trade_dts),
		extract(day from trade_dts),
		extract(hour from trade_dts),
		extract(minute from trade_dts),
		extract(second from trade_dts)
	INTO	rs;
	RETURN rs;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 3
 * Responsible for computing the amount of tax due by the customer as a result
 * of the trade
 * 
 */

CREATE OR REPLACE FUNCTION TradeResultFrame3(
				IN buy_value	numeric(12,2),
				IN cust_id	IDENT_T,
				IN sell_value	numeric(12,2),
				IN trade_id	TRADE_T,
				IN tax_amnt	VALUE_T) RETURNS VALUE_T AS $$
DECLARE
	-- Local Frame variables
	tax_rates	S_PRICE_T;
	tax_amount	VALUE_T;
BEGIN
	tax_amount = tax_amnt;

	SELECT	sum(TX_RATE)
	INTO	tax_rates
	FROM	TAXRATE
	WHERE	TX_ID IN ( SELECT	CX_TX_ID
			FROM	CUSTOMER_TAXRATE
			WHERE	CX_C_ID = cust_id);

	tax_amount = tax_rates * (sell_value - buy_value);

	UPDATE	TRADE
	SET	T_TAX = tax_amount
	WHERE	T_ID = trade_id;

	RETURN tax_amount;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 4
 * responsible for computing the commission for the broker who executed the
 * trade.
 * 
 */

CREATE OR REPLACE FUNCTION TradeResultFrame4(
				IN cust_id	IDENT_T,
				IN symbol	char(15),
				IN trade_qty	S_QTY_T,
				IN type_id	char(3)) RETURNS record AS $$
DECLARE
	-- Local Frame variables
	cust_tier	smallint;
	sec_ex_id	char(6);
	rs		RECORD;

	-- output parameters
	comm_rate	numeric(5,2);
	sec_name	varchar;
	
BEGIN
	SELECT	S_EX_ID,
		S_NAME
	INTO	sec_ex_id,
		sec_name
	FROM	SECURITY
	WHERE	S_SYMB = symbol;

	SELECT	C_TIER
	INTO	cust_tier
	FROM	CUSTOMER
	WHERE	C_ID = cust_id;

	-- Only want 1 commission rate row
	SELECT	CR_RATE
	INTO	comm_rate
	FROM	COMMISSION_RATE
	WHERE	CR_C_TIER = cust_tier AND
		CR_TT_ID = type_id AND
		CR_EX_ID = sec_ex_id AND
		CR_FROM_QTY <= trade_qty AND
		CR_TO_QTY >= trade_qty
	LIMIT 1;

	SELECT	comm_rate,
		sec_name
	INTO	rs;
	
	RETURN rs;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 5
 * responsible for recording the result of the trade and the broker's
 * commission.
 * 
 */

CREATE OR REPLACE FUNCTION TradeResultFrame5(
				IN broker_id		IDENT_T,
				IN comm_amount		numeric(5,2),
				IN st_completed_id	char(4),
				IN trade_dts		timestamp,
				IN trade_id		IDENT_T,
				IN trade_price		S_PRICE_T) RETURNS integer AS $$
DECLARE
	
BEGIN
	UPDATE	TRADE
	SET	T_COMM = comm_amount,
		T_DTS = trade_dts,
		T_ST_ID = st_completed_id,
		T_TRADE_PRICE = trade_price
	WHERE	T_ID = trade_id;

	INSERT INTO	TRADE_HISTORY (
					TH_T_ID,
					TH_DTS,
					TH_ST_ID)
	VALUES (trade_id, trade_dts, st_completed_id);
	
	UPDATE	BROKER
	SET	B_COMM_TOTAL = B_COMM_TOTAL + comm_amount,
		B_NUM_TRADES = B_NUM_TRADES + 1
	WHERE	B_ID = broker_id;
	
	RETURN 0;
END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 6
 * responsible for settling the trade.
 * 
 */

CREATE OR REPLACE FUNCTION TradeResultFrame6(
				IN acct_id		IDENT_T,
				IN due_date		timestamp,
				IN s_name		varchar,
				IN se_amount		VALUE_T,
				IN trade_dts		timestamp,
				IN trade_id		IDENT_T,
				IN trade_is_cash	smallint,
				IN trade_qty		S_QTY_T,
				IN type_name		char(12)) RETURNS BALANCE_T AS $$
DECLARE
	-- Local Frame Variables
	cash_type	char(40);

	-- output parameter
	acct_bal		BALANCE_T;
BEGIN
	IF trade_is_cash THEN
		cash_type = 'Cash Account';
	ELSE
		cash_type = 'Margin';
	END IF;

	INSERT INTO SETTLEMENT (SE_T_ID, SE_CASH_TYPE, SE_CASH_DUE_DATE, SE_AMT)
	VALUES (trade_id, cash_type, due_date, se_amount);

	IF trade_is_cash THEN
		UPDATE	CUSTOMER_ACCOUNT
		SET	CA_BAL = (CA_BAL + se_amount)
		WHERE	CA_ID = acct_id;

		INSERT INTO CASH_TRANSACTION (CT_DTS, CT_T_ID, CT_AMT, CT_NAME)
		VALUES (trade_dts, trade_id, se_amount,
		        (type_name || ' ' || trade_qty || ' shares of ' || s_name) );
	END IF;

	SELECT	CA_BAL
	INTO	acct_bal
	FROM	CUSTOMER_ACCOUNT
	WHERE	CA_ID = acct_id;

	RETURN	acct_bal;
END;
$$ LANGUAGE 'plpgsql';
