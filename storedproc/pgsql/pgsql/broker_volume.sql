-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
-- 
-- Copyright The DBT-5 Authors
-- 
-- Based on TPC-E Standard Specification Revision 1.14.0.
-- Clause 3.3.1.3
CREATE OR REPLACE FUNCTION BrokerVolumeFrame1 (
    IN broker_list VARCHAR(100)[40]
  , IN sector_name VARCHAR(30)
  , OUT broker_name VARCHAR(100)[]
  , OUT list_len INTEGER
  , OUT volume S_PRICE_T[]
) RETURNS RECORD
AS $$
DECLARE
    r RECORD;
BEGIN
    list_len := 0;
    broker_name := '{}';
    volume := '{}';
    FOR r IN
        SELECT b_name
             , sum(tr_qty * tr_bid_price) AS vol
        FROM trade_request
           , sector
           , industry
           , company
           , broker
           , security
        WHERE tr_b_id = b_id
          AND tr_s_symb = s_symb
          AND s_co_id = co_id
          AND co_in_id = in_id
          AND sc_id = in_sc_id
          AND b_name = ANY (broker_list)
          AND sc_name = sector_name
        GROUP BY b_name
        ORDER BY 2 DESC
    LOOP
        list_len := list_len + 1;
        broker_name[list_len] = r.b_name;
        volume[list_len] = r.vol;
    END LOOP;
END;
$$
LANGUAGE 'plpgsql';
