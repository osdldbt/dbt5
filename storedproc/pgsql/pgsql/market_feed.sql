/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0.
 */

-- Clause 3.3.3.3
CREATE OR REPLACE FUNCTION MarketFeedFrame1 (
    IN price_quote S_PRICE_T[20],
						IN status_submitted	char(4),
						IN symbol		char(15)[20],
    IN trade_qty S_QTY_T[20],
						IN type_limit_buy	char(3),
						IN type_limit_sell	char(3),
    IN type_stop_loss CHAR(3)
  , OUT num_updated INTEGER
  , OUT send_len INTEGER
  , OUT req_trade_symbol CHAR(15)[]
  , OUT req_trade_id TRADE_T[]
  , OUT req_price_quote S_PRICE_T[]
  , OUT req_trade_qty S_QTY_T[]
  , OUT req_trade_type CHAR(3)[]
) RETURNS RECORD AS $$
<<mff1>>
DECLARE
	-- variables
	i			integer;
	now_dts			timestamp;
	request_list		refcursor;
	trade_id		TRADE_T;
	price			numeric(8,2);
	trade_type		char(3);
	trade_qty		integer;
	irow_count INTEGER;
	tmp_symbol CHAR(15);
BEGIN
	now_dts = now();

	FOR i IN 1..20 LOOP
		-- start transaction
		UPDATE	LAST_TRADE
		SET	LT_PRICE = price_quote[i],
			LT_VOL = LT_VOL + MarketFeedFrame1.trade_qty[i],
			LT_DTS = now_dts
		WHERE	LT_S_SYMB = symbol[i];

		OPEN request_list FOR
		SELECT	TR_T_ID,
			TR_BID_PRICE,
			TR_TT_ID,
			TR_QTY,
			TR_S_SYMB
		FROM	TRADE_REQUEST
		WHERE	TR_S_SYMB = symbol[i] and 
			((TR_TT_ID = type_stop_loss and TR_BID_PRICE >= price_quote[i]) or
			(TR_TT_ID = type_limit_sell and	TR_BID_PRICE <= price_quote[i]) or
			(TR_TT_ID = type_limit_buy and TR_BID_PRICE >= price_quote[i]));

		FETCH	request_list
		INTO	trade_id,
			price,
			trade_type,
			mff1.trade_qty,
			tmp_symbol;

		i := i;
		WHILE FOUND LOOP
			UPDATE	TRADE
			SET	T_DTS = now_dts,
				T_ST_ID = status_submitted
			WHERE	T_ID = trade_id;

			GET DIAGNOSTICS irow_count = ROW_COUNT;
			num_updated = num_updated + irow_count;

			DELETE	FROM TRADE_REQUEST
			WHERE	TR_T_ID = trade_id;

			INSERT INTO TRADE_HISTORY
			VALUES (trade_id, now_dts, status_submitted);

			req_trade_symbol[i] := tmp_symbol;
			req_trade_id[i] := trade_id;
			req_price_quote[i] := price;
			req_trade_qty[i] := mff1.trade_qty;
			req_trade_type[i] := trade_type;

			i := i + 1;
		END LOOP;
	
		CLOSE request_list;
		-- commit transaction
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
