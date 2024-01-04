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
	j			integer;
	now_dts			timestamp;
	request_list RECORD;
	irow_count INTEGER;
BEGIN
	now_dts := now();
    num_updated := 0;
    j := 1;

	FOR i IN 1..20 LOOP
		-- start transaction
		UPDATE	LAST_TRADE
		SET	LT_PRICE = price_quote[i],
			LT_VOL = LT_VOL + MarketFeedFrame1.trade_qty[i],
			LT_DTS = now_dts
		WHERE	LT_S_SYMB = symbol[i];

        GET DIAGNOSTICS irow_count = ROW_COUNT;
        num_updated = num_updated + irow_count;

        FOR request_list IN
            SELECT tr_t_id
                 , tr_bid_price
                 , tr_tt_id
                 , tr_qty
            FROM trade_request
            WHERE tr_s_symb = symbol[i]
              AND (
                      (tr_tt_id = type_stop_loss AND tr_bid_price >= price_quote[i])
                   OR (tr_tt_id = type_limit_sell AND tr_bid_price <= price_quote[i])
                   OR (tr_tt_id = type_limit_buy AND tr_bid_price >= price_quote[i])
                  )
        LOOP
			UPDATE	TRADE
			SET	T_DTS = now_dts,
				T_ST_ID = status_submitted
			WHERE t_id = request_list.tr_t_id;

			DELETE	FROM TRADE_REQUEST
			WHERE tr_t_id = request_list.tr_t_id;

			INSERT INTO TRADE_HISTORY
			VALUES (request_list.tr_t_id, now_dts, status_submitted);

			req_trade_symbol[j] := symbol[i];
			req_trade_id[j] := request_list.tr_t_id;
			req_price_quote[j] := request_list.tr_bid_price;
			req_trade_type[j] := request_list.tr_tt_id;
			req_trade_qty[j] := request_list.tr_qty;

			j := j + 1;
		END LOOP;
		-- commit transaction
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
