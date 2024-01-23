-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
--
-- Copyright The DBT-5 Authors
--
-- Based on TPC-E Standard Specification Revision 1.14.0
-- 3.3.8.3
CREATE OR REPLACE FUNCTION TradeResultFrame1 (
    IN trade_id TRADE_T
  , OUT acct_id IDENT_T
  , OUT charge VALUE_T
  , OUT hs_qty S_QTY_T
  , OUT is_lifo SMALLINT
  , OUT num_found INTEGER
  , OUT symbol CHAR(15)
  , OUT trade_is_cash SMALLINT
  , OUT trade_qty S_QTY_T
  , OUT type_id CHAR(3)
  , OUT type_is_market SMALLINT
  , OUT type_is_sell SMALLINT
  , OUT type_name CHAR(12)
) RETURNS RECORD
AS $$
DECLARE
    -- variables
    rs RECORD;
BEGIN
    SELECT t_ca_id
         , t_tt_id
         , t_s_symb
         , t_qty
         , t_chrg
         , CASE WHEN t_lifo THEN 1
                ELSE 0
           END
         , CASE WHEN t_is_cash THEN 1
                ELSE 0
           END
    INTO acct_id
      , type_id
      , symbol
      , trade_qty
      , charge
      , is_lifo
      , trade_is_cash
    FROM trade
    WHERE t_id = trade_id;
    GET DIAGNOSTICS num_found = ROW_COUNT;
    SELECT tt_name
        , CASE WHEN tt_is_sell THEN 1
               ELSE 0
          END
        , CASE WHEN tt_is_mrkt THEN 1
               ELSE 0
          END
    INTO type_name
      , type_is_sell
      , type_is_market
    FROM trade_type
    WHERE tt_id = type_id;
    SELECT holding_summary.hs_qty
    INTO TradeResultFrame1.hs_qty
    FROM holding_summary
    WHERE hs_ca_id = acct_id
      AND hs_s_symb = symbol;
    IF TradeResultFrame1.hs_qty IS NULL THEN
        -- no prior holdings exist
        TradeResultFrame1.hs_qty = 0;
    END IF;
END;
$$
LANGUAGE 'plpgsql';

-- Clause 3.3.8.4
CREATE OR REPLACE FUNCTION TradeResultFrame2 (
    IN acct_id IDENT_T
  , IN hs_qty S_QTY_T
  , IN is_lifo SMALLINT
  , IN symbol CHAR(15)
  , IN trade_id TRADE_T
  , IN trade_price S_PRICE_T
  , IN trade_qty S_QTY_T
  , IN type_is_sell SMALLINT
  , OUT broker_id IDENT_T
  , OUT buy_value NUMERIC(12, 2)
  , OUT cust_id IDENT_T
  , OUT sell_value NUMERIC(12, 2)
  , OUT tax_status SMALLINT
  , OUT trade_dts TIMESTAMP
) RETURNS record
AS $$
DECLARE
    -- variables
    hold_id IDENT_T;
    hold_price S_PRICE_T;
    hold_qty S_QTY_T;
    needed_qty S_QTY_T;
    rs RECORD;
    -- cursor
    hold_list REFCURSOR;
BEGIN
    -- Get the timestamp
    SELECT now() INTO trade_dts;
    -- Initialize variables
    buy_value = 0.0;
    sell_value = 0.0;
    needed_qty = trade_qty;
    SELECT ca_b_id
         , ca_c_id
         , ca_tax_st
    INTO broker_id
       , cust_id
       , tax_status
    FROM customer_account
    WHERE ca_id = acct_id;
    -- Determine if sell or buy order
    IF type_is_sell THEN
        IF TradeResultFrame2.hs_qty = 0 THEN
            -- no prior holdings exist, but one will be inserted
            INSERT INTO holding_summary (
                hs_ca_id
              , hs_s_symb
              , hs_qty
            )
            VALUES (
                acct_id
              , symbol
              , (-1) * trade_qty
            );
        ELSE
            IF TradeResultFrame2.hs_qty != trade_qty THEN
                UPDATE holding_summary
                SET hs_qty = (TradeResultFrame2.hs_qty::INTEGER - trade_qty::INTEGER)
                WHERE hs_ca_id = acct_id
                  AND hs_s_symb = symbol;
            END IF;
        END IF;
        -- Sell Trade:
        -- First look for existing holdings, H_QTY > 0
        IF TradeResultFrame2.hs_qty > 0 THEN
            IF is_lifo THEN
                -- Could return 0, 1 or many rows
                OPEN hold_list FOR
                    SELECT h_t_id
                         , h_qty
                         , h_price
                    FROM holding
                    WHERE h_ca_id = acct_id
                        AND h_s_symb = symbol
                    ORDER BY h_dts DESC;
            ELSE
                -- Could return 0, 1 or many rows
                OPEN hold_list FOR
                    SELECT h_t_id
                         , h_qty
                         , h_price
                    FROM holding
                    WHERE h_ca_id = acct_id
                        AND h_s_symb = symbol
                    ORDER BY h_dts ASC;
            END IF;
            -- Liquidate existing holdings. Note that more than
            -- 1 HOLDING record can be deleted here since customer
            -- may have the same security with differing prices.
            WHILE needed_qty > 0 LOOP
                FETCH hold_list
                INTO hold_id
                   , hold_qty
                   , hold_price;
                EXIT WHEN NOT FOUND;
                IF hold_qty > needed_qty THEN
                    -- Selling some of the holdings
                    INSERT INTO holding_history (
                        hh_h_t_id
                      , hh_t_id
                      , hh_before_qty
                      , hh_after_qty
                    )
                    VALUES (
                        hold_id
                      , trade_id
                      , hold_qty
                      , (hold_qty - needed_qty)
                    );
                    UPDATE holding
                    SET h_qty = (hold_qty - needed_qty)
                    WHERE h_t_id = hold_id;
                    -- current of hold_list;
                    buy_value = buy_value + (needed_qty * hold_price);
                    sell_value = sell_value + (needed_qty * trade_price);
                    needed_qty = 0;
                ELSE
                    -- Selling all holdings
                    INSERT INTO holding_history (
                        hh_h_t_id
                      , hh_t_id
                      , hh_before_qty
                      , hh_after_qty
                    )
                    VALUES (
                        hold_id
                      , trade_id
                      , hold_qty
                      , 0
                    );
                    -- H_QTY after delete
                    DELETE FROM holding
                    WHERE h_t_id = hold_id;
                    -- current of hold_list;
                    buy_value = buy_value + (hold_qty * hold_price);
                    sell_value = sell_value + (hold_qty * trade_price);
                    needed_qty = needed_qty - hold_qty;
                END IF;
            END LOOP;
            CLOSE hold_list;
        END IF;
        -- Sell Short:
        -- If needed_qty > 0 then customer has sold all existing
        -- holdings and customer is selling short. A new HOLDING
        -- record will be created with H_QTY set to the negative
        -- number of needed shares.
        IF needed_qty > 0 THEN
            INSERT INTO holding_history (
                hh_h_t_id
              , hh_t_id
              , hh_before_qty
              , hh_after_qty
            )
            VALUES (
                trade_id
               , trade_id
               , 0
               , (-1) * needed_qty
            );
            INSERT INTO HOLDING (
                h_t_id
              , h_ca_id
              , h_s_symb
              , h_dts
              , h_price
              , h_qty
            )
            VALUES (
                trade_id
              , acct_id
              , symbol
              , trade_dts
              , trade_price
              , (-1) * needed_qty
            );
        ELSE
            IF TradeResultFrame2.hs_qty = trade_qty THEN
                DELETE FROM holding_summary
                WHERE hs_ca_id = acct_id
                  AND hs_s_symb = symbol;
            END IF;
        END IF;
    ELSE
        -- The trade is a BUY
        IF TradeResultFrame2.hs_qty = 0 THEN
            -- no prior holdings exist, but one will be inserted
            INSERT INTO holding_summary (
                hs_ca_id
              , hs_s_symb
              , hs_qty
            )
            VALUES (
                acct_id
              , symbol
              , trade_qty
            );
        ELSE
            -- TradeResultFrame2.hs_qty != 0
            IF -TradeResultFrame2.hs_qty != trade_qty THEN
                UPDATE holding_summary
                SET hs_qty = TradeResultFrame2.hs_qty + trade_qty
                WHERE hs_ca_id = acct_id
                  AND hs_s_symb = symbol;
            END IF;
        END IF;
        -- Short Cover:
        -- First look for existing negative holdings, H_QTY < 0,
        -- which indicates a previous short sell. The buy trade
        -- will cover the short sell.
        IF TradeResultFrame2.hs_qty < 0 THEN
            IF is_lifo THEN
                -- Could return 0, 1 or many rows
                OPEN hold_list FOR
                    SELECT h_t_id
                         , h_qty
                         , h_price
                    FROM holding
                    WHERE h_ca_id = acct_id
                      AND h_s_symb = symbol
                    ORDER BY h_dts DESC;
            ELSE
                -- Could return 0, 1 or many rows
                OPEN hold_list FOR
                    SELECT h_t_id
                         , h_qty
                         , h_price
                    FROM holding
                    WHERE h_ca_id = acct_id
                      AND h_s_symb = symbol
                    ORDER BY h_dts ASC;
            END IF;
            -- Buy back securities to cover a short position.
            WHILE needed_qty > 0 LOOP
                FETCH hold_list
                INTO hold_id
                   , hold_qty
                   , hold_price;
                EXIT WHEN NOT FOUND;
                IF (hold_qty + needed_qty < 0) THEN
                    -- Buying back some of the Short Sell
                    INSERT INTO holding_history (
                        hh_h_t_id
                      , hh_t_id
                      , hh_before_qty
                      , hh_after_qty
                    )
                    VALUES (
                        hold_id
                      , trade_id
                      , hold_qty
                      , (hold_qty + needed_qty)
                    );
                    UPDATE holding
                    SET h_qty = (hold_qty + needed_qty)
                    WHERE h_t_id = hold_id;
                    -- current of hold_list;
                    sell_value = sell_value + (needed_qty * hold_price);
                    buy_value = buy_value + (needed_qty * trade_price);
                    needed_qty = 0;
                ELSE
                    -- Buying back all of the Short Sell
                    INSERT INTO holding_history (
                        hh_h_t_id
                      , hh_t_id
                      , hh_before_qty
                      , hh_after_qty
                    )
                    VALUES (
                        hold_id
                      , trade_id
                      , hold_qty
                      , 0
                    );
                    DELETE FROM holding
                    WHERE h_t_id = hold_id;
                    -- current of hold_list;
                    -- Make hold_qty positive for easy calculations
                    hold_qty = - hold_qty;
                    sell_value = sell_value + (hold_qty * hold_price);
                    buy_value = buy_value + (hold_qty * trade_price);
                    needed_qty = needed_qty - hold_qty;
                END IF;
            END LOOP;
            CLOSE hold_list;
        END IF;
        -- Buy Trade:
        -- If needed_qty > 0, then the customer has covered all
        -- previous Short Sells and the customer is buying new
        -- holdings. A new HOLDING record will be created with
        -- H_QTY set to the number of needed shares.
        IF needed_qty > 0 THEN
            INSERT INTO holding_history (
                hh_h_t_id
              , hh_t_id
              , hh_before_qty
              , hh_after_qty
            )
            VALUES (
                trade_id
              , trade_id
              , 0
              , needed_qty
            );
            INSERT INTO holding (
                h_t_id
              , h_ca_id
              , h_s_symb
              , h_dts
              , h_price
              , h_qty
            )
            VALUES (
                trade_id
              , acct_id
              , symbol
              , trade_dts
              , trade_price
              , needed_qty
            );
        ELSE
            IF (-TradeResultFrame2.hs_qty = trade_qty) THEN
                DELETE FROM holding_summary
                WHERE hs_ca_id = acct_id
                    AND hs_s_symb = symbol;
            END IF;
        END IF;
    END IF;
END;
$$
LANGUAGE 'plpgsql';

-- Clause 3.3.8.5
CREATE OR REPLACE FUNCTION TradeResultFrame3 (
    IN buy_value VALUE_T
  , IN cust_id IDENT_T
  , IN sell_value VALUE_T
  , IN trade_id TRADE_T
  , OUT tax_amount VALUE_T
) RETURNS VALUE_T
AS $$
DECLARE
    -- Local Frame variables
    tax_rates VALUE_T;
BEGIN
    SELECT sum(tx_rate)
    INTO tax_rates
    FROM taxrate
    WHERE tx_id IN (
                       SELECT cx_tx_id
                       FROM customer_taxrate
                       WHERE cx_c_id = cust_id
                   );
    tax_amount := (sell_value - buy_value) * tax_rates;
    UPDATE trade
    SET t_tax = tax_amount
    WHERE t_id = trade_id;
END;
$$
LANGUAGE 'plpgsql';

-- Clause 3.3.8.6
CREATE OR REPLACE FUNCTION TradeResultFrame4 (IN cust_id IDENT_T
  , IN symbol CHAR(15)
  , IN trade_qty S_QTY_T
  , IN type_id CHAR(3)
  , OUT comm_rate NUMERIC(5, 2)
  , OUT s_name VARCHAR
) RETURNS RECORD
AS $$
DECLARE
    -- Local Frame variables
    cust_tier SMALLINT;
    sec_ex_id CHAR(6);
    rs RECORD;
BEGIN
    SELECT s_ex_id
         , security.s_name
    INTO sec_ex_id
       , TradeResultFrame4.s_name
    FROM security
    WHERE s_symb = symbol;
    SELECT c_tier
    INTO cust_tier
    FROM customer
    WHERE c_id = cust_id;
    -- Only want 1 commission rate row
    SELECT cr_rate
    INTO comm_rate
    FROM commission_rate
    WHERE cr_c_tier = cust_tier
      AND cr_tt_id = type_id
      AND cr_ex_id = sec_ex_id
      AND cr_from_qty <= trade_qty
      AND cr_to_qty >= trade_qty
    LIMIT 1;
END;
$$
LANGUAGE 'plpgsql';

-- Clause 3.3.8.7
CREATE OR REPLACE FUNCTION TradeResultFrame5 (
    IN broker_id IDENT_T
  , IN comm_amount VALUE_T
  , IN st_completed_id CHAR(4)
  , IN trade_dts TIMESTAMP
  , IN trade_id IDENT_T
  , IN trade_price S_PRICE_T
) RETURNS INTEGER
AS $$
DECLARE
BEGIN
    UPDATE trade
    SET t_comm = comm_amount
      , t_dts = trade_dts
      , t_st_id = st_completed_id
      , t_trade_price = trade_price
    WHERE t_id = trade_id;
    INSERT INTO trade_history (
        th_t_id
      , th_dts
      , th_st_id
    )
    VALUES (
        trade_id
      , trade_dts
      , st_completed_id
    );
    UPDATE broker
    SET b_comm_total = b_comm_total + comm_amount
      , b_num_trades = b_num_trades + 1
    WHERE b_id = broker_id;
    RETURN 0;
END;
$$
LANGUAGE 'plpgsql';

-- Clause 3.3.8.8
CREATE OR REPLACE FUNCTION TradeResultFrame6 (
    IN acct_id IDENT_T
  , IN due_date TIMESTAMP
  , IN s_name VARCHAR(70)
  , IN se_amount VALUE_T
  , IN trade_dts TIMESTAMP
  , IN trade_id IDENT_T
  , IN trade_is_cash SMALLINT
  , IN trade_qty S_QTY_T
  , IN type_name CHAR(12)
  , OUT acct_bal BALANCE_T
) RETURNS BALANCE_T
AS $$
DECLARE
    -- Local Frame Variables
    cash_type CHAR(40);
BEGIN
    IF trade_is_cash THEN
        cash_type = 'Cash Account';
    ELSE
        cash_type = 'Margin';
    END IF;
    INSERT INTO settlement (
        se_t_id
      , se_cash_type
      , se_cash_due_date
      , se_amt
    )
    VALUES (
        trade_id
      , cash_type
      , due_date
      , se_amount
    );
    IF trade_is_cash THEN
        UPDATE customer_account
        SET ca_bal = (ca_bal + se_amount)
        WHERE ca_id = acct_id;
        INSERT INTO cash_transaction (
            ct_dts
          , ct_t_id
          , ct_amt
          , ct_name
        )
        VALUES (
            trade_dts
          , trade_id
          , se_amount
          , (type_name || ' ' || trade_qty || ' shares of ' || s_name)
    );
    END IF;
    SELECT ca_bal
    INTO acct_bal
    FROM customer_account
    WHERE ca_id = acct_id;
END;
$$
LANGUAGE 'plpgsql';
