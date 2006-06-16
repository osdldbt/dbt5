/*
 * 2006 Rilson Nascimento
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2c Clause 3.3.5.
 */

CREATE OR REPLACE FUNCTION TradeStatusFrame1 (IN in_acct_id IDENT_T) RETURNS SETOF record AS $$
DECLARE
	out_c_l_name VARCHAR;
	out_c_f_name VARCHAR;
	out_b_name VARCHAR;
	rs RECORD;
BEGIN
	/*
	 * Retrieve information on the 50 most recent trades
	 */

	SELECT  C_L_NAME, 
		C_F_NAME, 
		B_NAME 
	INTO	out_c_l_name, out_c_f_name, out_b_name
	FROM    CUSTOMER_ACCOUNT, 
		CUSTOMER, 
		BROKER 
	WHERE   CA_ID   = in_acct_id AND 
		C_ID    = CA_C_ID AND 
		B_ID    = CA_B_ID;

	FOR rs IN
		SELECT  out_c_l_name,
			out_c_f_name,
			out_b_name,
			T_CHRG,
			T_EXEC_NAME,
			EX_NAME,
			S_NAME,
			ST_NAME,
			T_S_SYMB,
			T_DTS,
			T_ID,
			T_QTY,
			TT_NAME 
		FROM    TRADE, 
			STATUS_TYPE, 
			TRADE_TYPE,
			SECURITY,
			EXCHANGE
		WHERE   T_CA_ID = in_acct_id AND 
			ST_ID   = T_ST_ID AND 
			TT_ID   = T_TT_ID AND
			S_SYMB  = T_S_SYMB AND
			EX_ID   = S_EX_ID
		ORDER   BY T_DTS DESC
		LIMIT 50
	LOOP
		RETURN NEXT rs;
	END LOOP;

END;
$$ LANGUAGE 'plpgsql';
