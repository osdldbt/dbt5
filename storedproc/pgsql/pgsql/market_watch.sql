-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
--
-- Copyright The DBT-5 Authors
--
-- Based on TPC-E Standard Specification Revision 1.14.0
-- Clause 3.3.4.3
CREATE OR REPLACE FUNCTION MarketWatchFrame1 (
    IN acct_id IDENT_T
  , IN cust_id IDENT_T
  , IN ending_co_id IDENT_T
  , IN industry_name VARCHAR(50)
  , IN start_date DATE
  , IN starting_co_id IDENT_T
  , OUT pct_change DOUBLE PRECISION
) RETURNS DOUBLE PRECISION
AS $$
DECLARE
    -- variables
    old_mkt_cap DOUBLE PRECISION;
    new_mkt_cap DOUBLE PRECISION;
    symbol CHAR(15);
    sec_num_out S_COUNT_T;
    old_price DOUBLE PRECISION;
    new_price DOUBLE PRECISION;
    -- cursor
    stock_list REFCURSOR;
BEGIN
    IF cust_id != 0 THEN
        OPEN stock_list FOR
            SELECT wi_s_symb
            FROM watch_item
               , watch_list
            WHERE wi_wl_id = wl_id
              AND wl_c_id = cust_id;
    ELSIF industry_name != '' THEN
        OPEN stock_list FOR
            SELECT s_symb
            FROM industry
               , company
               , security
            WHERE in_name = industry_name
              AND co_in_id = in_id
              AND co_id BETWEEN starting_co_id AND ending_co_id
              AND s_co_id = co_id;
    ELSIF acct_id != 0 THEN
        OPEN stock_list FOR
            SELECT hs_s_symb
            FROM holding_summary
            WHERE hs_ca_id = acct_id;
    ELSE
        pct_change = 0.0;
        RETURN;
    END IF;
    old_mkt_cap = 0.0;
    new_mkt_cap = 0.0;
    pct_change = 0.0;
    FETCH stock_list INTO symbol;
    WHILE FOUND LOOP
        SELECT lt_price
        INTO new_price
        FROM last_trade
        WHERE lt_s_symb = symbol;
        SELECT s_num_out
        INTO sec_num_out
        FROM security
        WHERE s_symb = symbol;
        -- Only want one row, the most recent closing price for this security.
        SELECT dm_close
        INTO old_price
        FROM daily_market
        WHERE dm_s_symb = symbol
        ORDER BY dm_date DESC
        LIMIT 1;
        old_mkt_cap = old_mkt_cap + (sec_num_out * old_price);
        new_mkt_cap = new_mkt_cap + (sec_num_out * new_price);
        FETCH stock_list INTO symbol;
    END LOOP;
    IF old_mkt_cap != 0 THEN
        pct_change = 100 * ((new_mkt_cap / old_mkt_cap) - 1);
    ELSE
        pct_change = 0;
    END IF;
    CLOSE stock_list;
END;
$$
LANGUAGE 'plpgsql';
