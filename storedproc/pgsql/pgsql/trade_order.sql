/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0
 */

-- 3.3.7.3
CREATE OR REPLACE FUNCTION TradeOrderFrame1 (
    IN acct_id IDENT_T
  , OUT acct_name VARCHAR(50)
  , OUT broker_id IDENT_T
  , OUT broker_name VARCHAR(100)
  , OUT cust_f_name VARCHAR(30)
  , OUT cust_id IDENT_T
  , OUT cust_l_name VARCHAR(30)
  , OUT cust_tier SMALLINT
  , OUT num_found INTEGER
  , OUT tax_id VARCHAR(20)
  , OUT tax_status SMALLINT
) RETURNS RECORD AS $$
DECLARE
	-- variables
	rs 		RECORD;
BEGIN
	-- Get account, customer, and broker information
	SELECT	CA_NAME,
		CA_B_ID,
		CA_C_ID,
		CA_TAX_ST
	INTO	acct_name,
		broker_id,
		cust_id,
		tax_status
	FROM	CUSTOMER_ACCOUNT
	WHERE	CA_ID = acct_id;

    GET DIAGNOSTICS num_found = ROW_COUNT;

	SELECT	C_F_NAME,
		C_L_NAME,
		C_TIER,
		C_TAX_ID
	INTO	cust_f_name,
		cust_l_name,
		cust_tier,
		tax_id
	FROM	CUSTOMER
	WHERE	C_ID = cust_id;

	SELECT	B_NAME
	INTO	broker_name
	FROM	BROKER
	WHERE	B_ID=broker_id;
END;
$$ LANGUAGE 'plpgsql';

-- Clause 3.3.7.4
CREATE OR REPLACE FUNCTION TradeOrderFrame2(
				IN acct_id 	IDENT_T, 
    IN exec_f_name VARCHAR(30)
  , IN exec_l_name VARCHAR(30)
  , IN exec_tax_id VARCHAR(20)
  , OUT ap_acl VARCHAR(4)
) RETURNS VARCHAR(4) AS $$
BEGIN
    SELECT account_permission.ap_acl
    INTO TradeOrderFrame2.ap_acl
    FROM account_permission
    WHERE ap_ca_id = acct_id
      AND ap_f_name = exec_f_name
      AND ap_l_name = exec_l_name
      AND ap_tax_id = exec_tax_id;
END;
$$ LANGUAGE 'plpgsql';

-- 3.3.7.5
CREATE OR REPLACE FUNCTION TradeOrderFrame3(
				IN acct_id		IDENT_T,
				IN cust_id		IDENT_T,
				IN cust_tier		smallint,
				IN is_lifo		smallint,
				IN issue		char(6),
				IN st_pending_id	char(4),
				IN st_submitted_id	char(4),
				IN tax_status		smallint,
				IN trade_qty		S_QTY_T,
				IN trade_type_id	char(3),
				IN type_is_margin	smallint
  , INOUT co_name VARCHAR(60)
  , INOUT requested_price S_PRICE_T
  , INOUT symbol VARCHAR(15)
  , OUT buy_value BALANCE_T
  , OUT charge_amount VALUE_T
  , OUT comm_rate S_PRICE_T
  , OUT acct_assets VALUE_T
  , OUT market_price S_PRICE_T
  , OUT s_name VARCHAR(70)
  , OUT sell_value BALANCE_T
  , OUT status_id CHAR(4)
  , OUT tax_amount VALUE_T
  , OUT type_is_market SMALLINT
  , OUT type_is_sell SMALLINT
) RETURNS RECORD AS $$
<<tof3>>
DECLARE
	-- variables
    co_id IDENT_T;
	exch_id		char(6);
	tax_rates	S_PRICE_T;
	acct_bal	BALANCE_T;
	hold_assets	S_PRICE_T;

	-- Local frame variables used when estimating impact of this trade on
	-- any current holdings of the same security.
	hold_price	S_PRICE_T;
	hold_qty	S_QTY_T;
	needed_qty	S_QTY_T;
    hs_qty S_QTY_T;

	-- cursor
	hold_list	refcursor;

    tmp_type_is_market BOOLEAN;
    tmp_type_is_sell BOOLEAN;
BEGIN
    hs_qty := 0;
    comm_rate := 0;
    hold_assets := NULL;

	-- Get information on the security
    IF symbol = '' THEN
        SELECT company.co_id
        INTO co_id
		FROM	COMPANY 
        WHERE company.co_name = TradeOrderFrame3.co_name;

		SELECT	S_EX_ID,
               security.s_name,
			S_SYMB
		INTO	exch_id,
             s_name
           , symbol
		FROM	SECURITY
        WHERE s_co_id = co_id AND
			S_ISSUE = issue;
	ELSE
		SELECT	S_CO_ID,
			S_EX_ID,
               security.s_name
		INTO co_id,
			exch_id,
             TradeOrderFrame3.s_name
		FROM	SECURITY
        WHERE S_SYMB = symbol;
		
		SELECT company.co_name
        INTO TradeOrderFrame3.co_name
		FROM	COMPANY
        WHERE company.co_id = tof3.co_id;
	END IF;

	-- Get current pricing information for the security
	SELECT 	LT_PRICE
	INTO	market_price
	FROM	LAST_TRADE
    WHERE LT_S_SYMB = symbol;
	
	-- Set trade characteristics based on the type of trade.
	SELECT	TT_IS_MRKT,
		TT_IS_SELL
    INTO tmp_type_is_market
      , tmp_type_is_sell
	FROM	TRADE_TYPE
	WHERE	TT_ID = trade_type_id;

    IF tmp_type_is_market THEN
        type_is_market := 1;
    ELSE
        type_is_market := 0;
    END IF;

    IF tmp_type_is_sell THEN
        type_is_sell := 1;
    ELSE
        type_is_sell := 0;
    END IF;

    -- If this is a limit-order, then the requested_price was passed in to the
    -- frame, but if this a market-order, then the requested_price needs to be
    -- set to the current market price.
	IF tmp_type_is_market THEN
		requested_price = market_price;
	END IF;

	-- Initialize variables
	buy_value = 0.0;
	sell_value = 0.0;
	needed_qty = trade_qty;

    -- No prior holdings exist – if no rows returned.
    SELECT coalesce(holding_summary.hs_qty, 0)
    INTO hs_qty
	FROM	HOLDING_SUMMARY
	WHERE	HS_CA_ID = acct_id AND
          HS_S_SYMB = symbol;

	IF tmp_type_is_sell THEN
	-- This is a sell transaction, so estimate the impact to any currently held
	-- long positions in the security.
        IF hs_qty > 0 THEN
			IF is_lifo = 1 THEN
				-- Estimates will be based on closing most recently acquired
				-- holdings
				-- Could return 0, 1 or many rows
				OPEN	hold_list FOR
				SELECT	H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
                      H_S_SYMB = symbol
				ORDER BY H_DTS DESC;
			ELSE
				-- Estimates will be based on closing oldest holdings
				-- Could return 0, 1 or many rows
				OPEN	hold_list FOR
				SELECT	H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
                      H_S_SYMB = symbol
				ORDER BY H_DTS ASC;
			END IF;

			-- Estimate, based on the requested price, any profit that may be
			-- realized by selling current holdings for this security. The
			-- customer may have multiple holdings for this security
			-- (representing different purchases of this security at different
			-- times and therefore, most likely, different prices).

			WHILE needed_qty > 0 LOOP
				FETCH	hold_list
				INTO	hold_qty,
					hold_price;
				EXIT WHEN NOT FOUND;

				IF hold_qty > needed_qty THEN
					-- Only a portion of this holding would be sold as a result
					-- of the trade.
					buy_value = buy_value + (needed_qty * hold_price);
					sell_value = sell_value + (needed_qty * requested_price);
					needed_qty = 0;
				ELSE
					-- All of this holding would be sold as a result of this
					-- trade.
					buy_value = buy_value + (hold_qty * hold_price);
					sell_value = sell_value + (hold_qty * requested_price);
					needed_qty = needed_qty - hold_qty;
				END IF;
			END LOOP;

			CLOSE hold_list;
		END IF;
		-- NOTE: If needed_qty is still greater than 0 at this point, then the
		-- customer would be liquidating all current holdings for this
		-- security, and then short-selling this remaining balance for the
		-- transaction.
	ELSE
		-- This is a buy transaction, so estimate the impact to any currently
		-- held short positions in the security. These are represented as
		-- negative H_QTY holdings. Short postions will be covered before
		-- opening a long postion in this security.

        IF hs_qty < 0 THEN  -- Existing short position to buy
			IF is_lifo = 1 THEN
				-- Estimates will be based on closing most recently acquired
				-- holdings
				-- Could return 0, 1 or many rows

				OPEN 	hold_list FOR
				SELECT	H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
                      H_S_SYMB = symbol
				ORDER BY H_DTS DESC;
			ELSE
				-- Estimates will be based on closing oldest holdings
				-- Could return 0, 1 or many rows

				OPEN	hold_list FOR
				SELECT	H_QTY,
					H_PRICE
				FROM	HOLDING
				WHERE	H_CA_ID = acct_id AND
                      H_S_SYMB = symbol
				ORDER BY H_DTS ASC;
			END IF;

			-- Estimate, based on the requested price, any profit that may be
			-- realized by covering short postions currently held for this
			-- security. The customer may have multiple holdings for this
			-- security (representing different purchases of this security at
			-- different times).

			WHILE needed_qty > 0 LOOP
				FETCH	hold_list
				INTO	hold_qty,
					hold_price;
				EXIT WHEN NOT FOUND;

				IF (hold_qty + needed_qty < 0) THEN
					-- Only a portion of this holding would be covered (bought
					-- back) as a result of this trade.
					sell_value = sell_value + (needed_qty * hold_price);
					buy_value = buy_value + (needed_qty * requested_price);
					needed_qty = 0;
				ELSE
					-- All of this holding would be covered (bought back) as
					-- a result of this trade.
					-- NOTE: Local variable hold_qty is made positive for easy
					-- calculations
					hold_qty = -hold_qty;
					sell_value = sell_value + (hold_qty * hold_price);
					buy_value = buy_value + (hold_qty * requested_price);
					needed_qty = needed_qty - hold_qty;
				END IF;
			END LOOP;

			CLOSE hold_list;
		END IF;
		-- NOTE: If needed_qty is still greater than 0 at this point, then the
		-- customer would cover all current short positions (if any) for this
        -- security, and then open a new long position for the remaining
        -- balance of this transaction.
	END IF;

	-- Estimate any capital gains tax that would be incurred as a result of this
	-- transaction.

	tax_amount = 0.0;

	IF (sell_value > buy_value) AND ((tax_status = 1) OR (tax_status = 2)) THEN
        -- Customers may be subject to more than one tax at different rates.
        -- Therefore, get the sum of the tax rates that apply to the customer
        -- and estimate the overall amount of tax that would result from this
        -- order.
		SELECT	sum(TX_RATE)
		INTO	tax_rates
		FROM	TAXRATE
		WHERE	TX_ID IN (
				SELECT	CX_TX_ID
				FROM	CUSTOMER_TAXRATE
				WHERE	CX_C_ID = cust_id);

		tax_amount = (sell_value - buy_value) * tax_rates;
	END IF;

	-- Get administrative fees (e.g. trading charge, commission rate)
	SELECT	CR_RATE
	INTO	comm_rate
	FROM	COMMISSION_RATE
	WHERE	CR_C_TIER = cust_tier AND
		CR_TT_ID = trade_type_id AND
		CR_EX_ID = exch_id AND
		CR_FROM_QTY <= trade_qty AND
		CR_TO_QTY >= trade_qty;

	SELECT	CH_CHRG
	INTO	charge_amount
	FROM	CHARGE
	WHERE	CH_C_TIER = cust_tier AND
		CH_TT_ID = trade_type_id;

	-- Compute assets on margin trades
    acct_assets = 0.0;

	IF type_is_margin = 1 THEN
		SELECT	CA_BAL
		INTO	acct_bal
		FROM	CUSTOMER_ACCOUNT
		WHERE	CA_ID = acct_id;

		-- Should return 0 or 1 row
        SELECT sum(holding_summary.hs_qty * lt_price)
		INTO	hold_assets
		FROM	HOLDING_SUMMARY,
			LAST_TRADE
		WHERE	HS_CA_ID = acct_id AND
			LT_S_SYMB = HS_S_SYMB;

		IF hold_assets is NULL THEN /* account currently has no holdings */
            acct_assets = acct_bal;
		ELSE
            acct_assets = hold_assets + acct_bal;
		END IF;
	END IF;

	-- Set the status for this trade
	IF tmp_type_is_market THEN
		status_id = st_submitted_id;
	ELSE
		status_id = st_pending_id;
	END IF;
END;
$$ LANGUAGE 'plpgsql';

-- Clause 3.3.7.6
CREATE OR REPLACE FUNCTION TradeOrderFrame4(
				IN acct_id            IDENT_T,
    IN broker_id IDENT_T,
				IN charge_amount      VALUE_T,
				IN comm_amount        VALUE_T,
				IN exec_name          char(64),
				IN is_cash            smallint,
				IN is_lifo            smallint,
				IN requested_price    S_PRICE_T,
				IN status_id          char(4),
				IN symbol             varchar(15),
				IN trade_qty          S_QTY_T,
				IN trade_type_id      char(3),
				IN type_is_market     smallint
  , OUT trade_id TRADE_T
) RETURNS TRADE_T AS $$
DECLARE
	-- variables
	now_dts		timestamp;
    tmp_is_cash BOOLEAN;
    tmp_is_lifo BOOLEAN;
BEGIN
	-- Get the timestamp
	SELECT	NOW()
	INTO	now_dts;

    IF is_cash = 1 THEN
        tmp_is_cash := TRUE;
    ELSE
        tmp_is_cash := FALSE;
    END IF;

    IF is_lifo = 1 THEN
        tmp_is_lifo := TRUE;
    ELSE
        tmp_is_lifo := FALSE;
    END IF;

	-- Record trade information in TRADE table.
	INSERT INTO TRADE (
			T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH,
			T_S_SYMB, T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME,
			T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, T_LIFO)
	VALUES 		(nextval('seq_trade_id'), now_dts, status_id, trade_type_id, 
           tmp_is_cash, symbol, trade_qty, requested_price, acct_id,
           exec_name, NULL, charge_amount, comm_amount, 0, tmp_is_lifo)
    RETURNING T_ID
    INTO trade_id;

	-- Record pending trade information in TRADE_REQUEST table if this trade
	-- is a limit trade

	IF type_is_market = 0 THEN
		INSERT INTO TRADE_REQUEST (
					TR_T_ID, TR_TT_ID, TR_S_SYMB,
            TR_QTY, TR_BID_PRICE, TR_B_ID)
		VALUES 			(trade_id, trade_type_id, symbol,
            trade_qty, requested_price, broker_id);
	END IF;

	-- Record trade information in TRADE_HISTORY table.
	INSERT INTO TRADE_HISTORY (
				TH_T_ID, TH_DTS, TH_ST_ID)
	VALUES (trade_id, now_dts, status_id);
END;
$$ LANGUAGE 'plpgsql';
