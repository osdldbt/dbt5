/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Trade Cleanup transaction
 * -------------------------
 * This transaction is used to cancel any pending or submitted trades that are
 * left in the database from a previous Test Run.
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.12.
 */

/*
 * Frame 1
 * Cancel pending and submitted trades
 */

CREATE OR REPLACE FUNCTION TradeCleanupFrame1 (
						IN st_canceled_id	char(4),
						IN st_pending_id	char(4),
						IN st_submitted_id	char(4),
						IN start_trade_id	TRADE_T) RETURNS smallint AS $$
DECLARE
	-- variables
	trade_id	TRADE_T;
	tr_trade_id	TRADE_T;
	now_dts		timestamp;

	pending_list	refcursor;
	submit_list	refcursor;
BEGIN
	/* Find pending trades from TRADE_REQUEST */

	OPEN pending_list FOR
	SELECT	TR_T_ID
	FROM	TRADE_REQUEST
	ORDER BY TR_T_ID;

	/* Insert a submitted followed by canceled record into TRADE_HISTORY, mark
       the trade canceled and delete the pending trade */

	FETCH	pending_list
	INTO	tr_trade_id;

	WHILE FOUND LOOP
		now_dts = now();

		INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_trade_id, now_dts, st_submitted_id);

		UPDATE	TRADE
		SET	T_ST_ID = st_canceled_id,
			T_DTS = now_dts
		WHERE	T_ID = tr_trade_id;

		INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (tr_trade_id, now_dts, st_canceled_id);

		FETCH	pending_list
		INTO	tr_trade_id;
	END LOOP;

	/* Remove all pending trades */

	DELETE FROM TRADE_REQUEST;

	/* Find submitted trades, change the status to canceled and insert a
       canceled record into TRADE_HISTORY*/

	OPEN submit_list FOR
	SELECT	T_ID
	FROM	TRADE
	WHERE	T_ID >= start_trade_id AND
		T_ST_ID = st_submitted_id;

	FETCH	submit_list
	INTO	trade_id;

	WHILE FOUND LOOP
		now_dts = now();

		/* Mark the trade as canceled, and record the time */

		UPDATE	TRADE
		SET	T_ST_ID = st_canceled_id,
			T_DTS = now_dts
		WHERE	T_ID = trade_id;

		INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
		VALUES (trade_id, now_dts, st_canceled_id);

		FETCH	submit_list
		INTO	trade_id;
	END LOOP;

	RETURN 0;
END;
$$ LANGUAGE 'plpgsql';
