/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Trade Status transaction
 * ------------------------
 * This transaction returns the status of fifty trades. A list of all trades
 * for a given customer's account is retrieved and the fifty most recently
 * placed trades are selected.
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2c Clause 3.3.5.
 */


/*
 * Frame 1
 * responsible for retrieving information on the 50 most recent trades
 */

CREATE OR REPLACE FUNCTION TradeStatusFrame1 (IN acct_id IDENT_T)
		RETURNS SETOF record AS $$
DECLARE
	-- output parameters
	cust_l_name VARCHAR;
	cust_f_name VARCHAR;
	broker_name VARCHAR;

	-- variables
	rs RECORD;
BEGIN
	-- Only want 50 rows, the 50 most recent trades for this customer account
	SELECT	C_L_NAME,
		C_F_NAME,
		B_NAME
	INTO 	cust_l_name,
		cust_f_name,
		broker_name
	FROM	CUSTOMER_ACCOUNT,
		CUSTOMER,
		BROKER
	WHERE	CA_ID = acct_id AND
		C_ID = CA_C_ID AND
		B_ID = CA_B_ID;

	FOR rs IN
		SELECT	cust_l_name,
			cust_f_name,
			broker_name,
			T_CHRG,
			T_EXEC_NAME,
			EX_NAME,
			S_NAME,
			ST_NAME,
			T_S_SYMB,
			extract(year from T_DTS),
			extract(month from T_DTS),
			extract(day from T_DTS),
			extract(hour from T_DTS),
			extract(minute from T_DTS),
			extract(second from T_DTS),
			T_ID,
			T_QTY,
			TT_NAME 
		FROM	TRADE,
			STATUS_TYPE,
			TRADE_TYPE,
			SECURITY,
			EXCHANGE
		WHERE	T_CA_ID = acct_id AND
			ST_ID = T_ST_ID AND
			TT_ID = T_TT_ID AND
			S_SYMB = T_S_SYMB AND
			EX_ID = S_EX_ID
		ORDER BY T_DTS desc
		LIMIT 50
	LOOP
		RETURN NEXT rs;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';
