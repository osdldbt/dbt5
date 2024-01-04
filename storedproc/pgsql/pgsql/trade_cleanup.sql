-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
--
-- Copyright The DBT-5 Authors
--
-- Trade Cleanup transaction
-- -------------------------
-- This transaction is used to cancel any pending or submitted trades that are
-- left in the database from a previous Test Run.
--
-- Based on TPC-E Standard Specification Revision 1.14.0 Clause 3.3.12.
--
-- Frame 1
-- Cancel pending and submitted trades
CREATE OR REPLACE FUNCTION TradeCleanupFrame1 (
    IN st_canceled_id CHAR(4)
  , IN st_pending_id CHAR(4)
  , IN st_submitted_id CHAR(4)
  , IN start_trade_id TRADE_T
) RETURNS SMALLINT
AS $$
DECLARE
    -- variables
    trade_id TRADE_T;
    tr_trade_id TRADE_T;
    now_dts TIMESTAMP;
    pending_list refcursor;
    submit_list refcursor;
BEGIN
    -- Find pending trades from TRADE_REQUEST
    OPEN pending_list FOR
        SELECT tr_t_id
        FROM trade_request
        ORDER BY tr_t_id;

    -- Insert a submitted followed by canceled record into TRADE_HISTORY, mark
    -- the trade canceled and delete the pending trade
    FETCH pending_list INTO tr_trade_id;
    WHILE FOUND LOOP
        now_dts = now();
        INSERT INTO trade_history (
            th_t_id
          , th_dts
          , th_st_id
        )
        VALUES (
            tr_trade_id
          , now_dts
          , st_submitted_id
         )
        ON CONFLICT DO NOTHING;
        UPDATE trade
        SET t_st_id = st_canceled_id
          , t_dts = now_dts
        WHERE t_id = tr_trade_id;
        INSERT INTO trade_history (
            th_t_id
          , th_dts
          , th_st_id
        )
        VALUES (
            tr_trade_id
          , now_dts
          , st_canceled_id
        )
        ON CONFLICT DO NOTHING;
        FETCH pending_list INTO tr_trade_id;
    END LOOP;

    -- Remove all pending trades
    DELETE FROM trade_request;

    -- Find submitted trades, change the status to canceled and insert a
    -- canceled record into TRADE_HISTORY
    OPEN submit_list FOR
        SELECT t_id
        FROM trade
        WHERE t_id >= start_trade_id
          AND t_st_id = st_submitted_id;
    FETCH submit_list INTO trade_id;
    WHILE FOUND LOOP
        now_dts := now();

        -- Mark the trade as canceled, and record the time
        UPDATE TRADE
        SET t_st_id = st_canceled_id
          , t_dts = now_dts
        WHERE t_id = trade_id;
        INSERT INTO trade_history (
            th_t_id
          , th_dts
          , th_st_id
        )
        VALUES (
            trade_id
          , now_dts
          , st_canceled_id
        )
        ON CONFLICT DO NOTHING;
        FETCH submit_list INTO trade_id;
    END LOOP;
    RETURN 0;
END;
$$
LANGUAGE 'plpgsql';
