/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0
 */

-- Clause 3.3.4.3
CREATE OR REPLACE FUNCTION MarketWatchFrame1 (
						IN acct_id		IDENT_T,
						IN cust_id		IDENT_T,
						IN ending_co_id 	IDENT_T,
    IN industry_name VARCHAR(50)
  , IN start_date DATE
  , IN starting_co_id IDENT_T
  , OUT pct_change DOUBLE PRECISION
) RETURNS DOUBLE PRECISION AS $$
DECLARE
	-- variables
    old_mkt_cap DOUBLE PRECISION;
    new_mkt_cap DOUBLE PRECISION;
	symbol		char(15);
	sec_num_out	S_COUNT_T;
    old_price DOUBLE PRECISION;
    new_price DOUBLE PRECISION;

	-- cursor
	stock_list	refcursor;
BEGIN
	IF cust_id != 0 THEN
		OPEN	stock_list FOR
            SELECT WI_S_SYMB
            FROM watch_item
               , watch_list
            WHERE wi_wl_id = wl_id
              AND wl_c_id = cust_id;
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
        pct_change = 0.0;
        RETURN;
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
END;
$$ LANGUAGE 'plpgsql';

