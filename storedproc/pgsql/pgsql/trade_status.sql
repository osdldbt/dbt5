-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
--
-- Copyright The DBT-5 Authors
--
-- Based on TPC-E Standard Specification Revision 1.14.0
-- Clause 3.3.9.3
CREATE OR REPLACE FUNCTION TradeStatusFrame1 (
    IN acct_id IDENT_T
  , OUT broker_name VARCHAR
  , OUT charge VALUE_T[50]
  , OUT cust_l_name VARCHAR(30)
  , OUT cust_f_name VARCHAR(30)
  , OUT ex_name VARCHAR(100)[50]
  , OUT exec_name VARCHAR(64)[50]
  , OUT num_found INTEGER
  , OUT s_name VARCHAR(70)[50]
  , OUT status_name VARCHAR(10)[50]
  , OUT symbol VARCHAR(15)[50]
  , OUT trade_dts TIMESTAMP[50]
  , OUT trade_id TRADE_T[50]
  , OUT trade_qty S_QTY_T[50]
  , OUT type_name VARCHAR(12)[50]
) RETURNS RECORD
    AS $$
DECLARE
    -- variables
    rs RECORD;
    i INTEGER;
BEGIN
    -- Only want 50 rows, the 50 most recent trades for this customer account
    i := 0;
    FOR rs IN
        SELECT t_id
             , t_dts
             , st_name
             , tt_name
             , t_s_symb
             , t_qty
             , t_exec_name
             , t_chrg
             , security.s_name
             , exchange.ex_name
        FROM trade
           , status_type
           , trade_type
           , security
           , exchange
        WHERE t_ca_id = acct_id
            AND st_id = t_st_id
            AND tt_id = t_tt_id
            AND s_symb = t_s_symb
            AND ex_id = s_ex_id
        ORDER BY t_dts DESC
        LIMIT 50
    LOOP
        i := i + 1;
        trade_id[i] := rs.t_id;
        trade_dts[i] := rs.t_dts;
        status_name[i] := rs.st_name;
        type_name[i] := rs.tt_name;
        symbol[i] := rs.t_s_symb;
        trade_qty[i] := rs.t_qty;
        exec_name[i] := rs.t_exec_name;
        charge[i] := rs.t_chrg;
        s_name[i] := rs.s_name;
        ex_name[i] := rs.ex_name;
    END LOOP;
    num_found := i;
    SELECT c_l_name
         , c_f_name
         , b_name
    INTO cust_l_name
       , cust_f_name
       , broker_name
    FROM customer_account
       , customer
       , broker
    WHERE ca_id = acct_id
      AND c_id = ca_c_id
      AND b_id = ca_b_id;
END;
$$
LANGUAGE 'plpgsql';
