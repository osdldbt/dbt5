/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Market Watch transaction
 * -------------------------
 * This transaction calculates the percentage change in value of the market
 * capitalization of collection of securities at yesterday’s closing price
 * compared to the current market price for that collection of securities.
 * 
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.10.
 */

/*
 * Frame 1
 * Build list of securities and compute percentage
 */

CREATE OR REPLACE FUNCTION MarketWatchFrame1 (
						IN acct_id		IDENT_T,
						IN cust_id		IDENT_T,
						IN ending_co_id 	IDENT_T,
						IN industry_name	varchar,
						IN starting_co_id 	IDENT_T) RETURNS record AS $$
DECLARE
	-- variables
	rs		RECORD;
	old_mkt_cap	double precision;
	new_mkt_cap	double precision;
	pct_change	double precision;
	symbol		char(15);
	sec_num_out	S_COUNT_T;
	old_price	S_PRICE_T;
	new_price	S_PRICE_T;

	-- cursor
	stock_list	refcursor;
BEGIN
	IF cust_id != 0 THEN
		OPEN	stock_list FOR
		SELECT	distinct WI_S_SYMB
		FROM	WATCH_ITEM
		WHERE	WI_WL_ID in (SELECT WL_ID
					FROM WATCH_LIST
					WHERE WL_C_ID = cust_id);
	ELSIF industry_name != '' THEN
		OPEN stock_list FOR
		SELECT	S_SYMB
		FROM	INDUSTRY,
			COMPANY,
			SECURITY
		WHERE	IN_NAME = industry_name AND
			CO_IN_ID = IN_ID AND
			CO_ID BETWEEN starting_co_id AND ending_co_id AND
			S_CO_ID = CO_ID;
	ELSIF acct_id != 0 THEN
		OPEN stock_list FOR
		SELECT	HS_S_SYMB
		FROM	HOLDING_SUMMARY
		WHERE	HS_CA_ID = acct_id;
	ELSE
		SELECT	-1::smallint, 0.0  -- status fail, pct_change
		INTO	rs;
		RETURN rs;
	END IF;

	old_mkt_cap = 0.0;
	new_mkt_cap = 0.0;
	pct_change = 0.0;

	FETCH	stock_list
	INTO	symbol;

	WHILE FOUND LOOP
		SELECT	LT_PRICE
		INTO	new_price
		FROM	LAST_TRADE
		WHERE	LT_S_SYMB = symbol;

		SELECT	S_NUM_OUT
		INTO	sec_num_out
		FROM	SECURITY
		WHERE	S_SYMB = symbol;
		
		-- Only want one row, the most recent closing price for this security.

		SELECT	DM_CLOSE
		INTO	old_price
		FROM	DAILY_MARKET
		WHERE	DM_S_SYMB = symbol
		ORDER BY DM_DATE desc
		LIMIT 1;

		old_mkt_cap = old_mkt_cap + (sec_num_out * old_price);
		new_mkt_cap = new_mkt_cap + (sec_num_out * new_price);

		FETCH	stock_list
		INTO	symbol;
	END LOOP;
	
	IF old_mkt_cap != 0 THEN
		pct_change = 100 * ( ( new_mkt_cap / old_mkt_cap ) - 1);
	ELSE
		pct_change = 0;
	END IF;
	
	CLOSE stock_list;

	SELECT	0::smallint, pct_change	-- status ok, pct_change
	INTO	rs;
	RETURN	rs;
END;
$$ LANGUAGE 'plpgsql';

