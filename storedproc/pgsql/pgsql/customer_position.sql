/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0
 */

-- Clause 3.3.2.3
CREATE OR REPLACE FUNCTION CustomerPositionFrame1 (
		INOUT cust_id IDENT_T,
		IN tax_id VARCHAR(20),
    OUT acct_id IDENT_T[10],
		OUT acct_len INTEGER,
		OUT asset_total DOUBLE PRECISION[10],
		OUT c_ad_id IDENT_T,
		OUT c_area_1 VARCHAR(3),
		OUT c_area_2 VARCHAR(3),
		OUT c_area_3 VARCHAR(3),
		OUT c_ctry_1 VARCHAR(3),
		OUT c_ctry_2 VARCHAR(3),
		OUT c_ctry_3 VARCHAR(3),
		OUT c_dob DATE,
		OUT c_email_1 VARCHAR(50),
		OUT c_email_2 VARCHAR(50),
		OUT c_ext_1 VARCHAR(5),
		OUT c_ext_2 VARCHAR(5),
		OUT c_ext_3 VARCHAR(5),
		OUT c_f_name VARCHAR(30),
		OUT c_gndr VARCHAR(1),
		OUT c_l_name VARCHAR(30),
		OUT c_local_1 VARCHAR(10),
		OUT c_local_2 VARCHAR(10),
		OUT c_local_3 VARCHAR(10),
		OUT c_m_name VARCHAR(1),
		OUT c_st_id VARCHAR(4),
		OUT c_tier SMALLINT,
    OUT cash_bal BALANCE_T[10]
) RETURNS RECORD AS $$
DECLARE
	r RECORD;
BEGIN
	IF cust_id = 0 THEN
		SELECT	C_ID
		INTO	cust_id
		FROM	CUSTOMER
		WHERE	C_TAX_ID = tax_id;
	END IF;

    SELECT customer.c_st_id
         , customer.c_l_name
         , customer.c_f_name
         , customer.c_m_name
         , customer.c_gndr
         , customer.c_tier
         , customer.c_dob
         , customer.c_ad_id
         , customer.c_ctry_1
         , customer.c_area_1
         , customer.c_local_1
         , customer.c_ext_1
         , customer.c_ctry_2
         , customer.c_area_2
         , customer.c_local_2
         , customer.c_ext_2
         , customer.c_ctry_3
         , customer.c_area_3
         , customer.c_local_3
         , customer.c_ext_3
         , customer.c_email_1
         , customer.c_email_2
    INTO c_st_id
       , c_l_name
       , c_f_name
       , c_m_name
       , c_gndr
       , c_tier
       , c_dob
       , c_ad_id
       , c_ctry_1
       , c_area_1
       , c_local_1
       , c_ext_1
       , c_ctry_2
       , c_area_2
       , c_local_2
       , c_ext_2
       , c_ctry_3
       , c_area_3
       , c_local_3
       , c_ext_3
       , c_email_1
       , c_email_2
	FROM	CUSTOMER
	WHERE	c_id = cust_id;

	-- Should return 1 to max_acct_len.
	acct_len := 0;
	FOR r IN
			SELECT CA_ID,
			       CA_BAL,
               coalesce(sum(hs_qty * lt_price), 0.00) AS soma
			FROM CUSTOMER_ACCOUNT left outer join
			     HOLDING_SUMMARY on HS_CA_ID = CA_ID,
			     LAST_TRADE
			WHERE CA_C_ID = cust_id
			  AND LT_S_SYMB = HS_S_SYMB
			GROUP BY CA_ID, CA_BAL
			ORDER BY 3 asc
			LIMIT 10
	LOOP
		acct_len := acct_len + 1;
		acct_id[acct_len] := r.CA_ID;
		cash_bal[acct_len] := r.CA_BAL;
        asset_total[acct_len] := r.soma;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';

-- Clause 3.3.2.4
CREATE OR REPLACE FUNCTION CustomerPositionFrame2(
		IN acct_id IDENT_T,
    OUT hist_dts TIMESTAMP[30],
		OUT hist_len INTEGER,
    OUT qty S_QTY_T[30],
    OUT symbol VARCHAR(15)[30],
    OUT trade_id IDENT_T[]
  , OUT trade_status VARCHAR(10)[30]
) RETURNS RECORD AS $$
DECLARE
	r		RECORD;
BEGIN
	hist_len := 0;
	FOR r IN
			SELECT T_ID,
			       T_S_SYMB,
			       T_QTY,
			       ST_NAME,
			       TH_DTS
			FROM (SELECT T_ID as ID
			      FROM TRADE
			      WHERE T_CA_ID = acct_id
			      ORDER BY T_DTS desc LIMIT 10) as T,
			     TRADE,
			     TRADE_HISTORY,
			     STATUS_TYPE
			WHERE T_ID = ID
			  AND TH_T_ID = T_ID
			  AND ST_ID = TH_ST_ID
			ORDER BY TH_DTS desc
			LIMIT 30
	LOOP
        hist_len := hist_len + 1;
		trade_id[hist_len] := r.t_id;
		symbol[hist_len] := r.t_s_symb;
		qty[hist_len] := r.t_qty;
		trade_status[hist_len] := r.st_name;
		hist_dts[hist_len] := r.th_dts;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
