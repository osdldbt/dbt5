/*
 * 2006 Rilson Nascimento
 *
 * Customer Position transaction
 * ------------------------
 * This transaction takes a customer tax ID or customer ID as input and is 
 * responsible for calculating the current market value of each one of that
 * customer’s accounts.
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.6.
 */

/*
 * Frame 1
 * retrieves the detailed customer information and finds the cash balance for
 * each of the customer’s accounts as well as the total value of the 
 * holdings in each account.
 */

CREATE OR REPLACE FUNCTION CustomerPositionFrame1 (
						IN cust_id	IDENT_T,
						IN tax_id	char(20),
						IN max_acct_len integer) RETURNS SETOF record AS $$
DECLARE
	-- output parameters
	customer_id	IDENT_T;
	acct_id		text;
	acct_bal	text;
	asset_total	text;
	acct_len	integer;

	-- variables
	i		integer;
	rs		RECORD;
BEGIN
	customer_id = cust_id;

	IF customer_id = 0 THEN
		SELECT	C_ID
		INTO	customer_id
		FROM	CUSTOMER
		WHERE	C_TAX_ID = tax_id;
	END IF;

	-- Should return 1 to max_acct_len.

	i = 1;
	acct_id = '';
	acct_bal = '';
	asset_total = '';

	FOR rs IN
		SELECT	CA_ID,
			CA_BAL,
			sum(HS_QTY * LT_PRICE) as soma
		FROM	CUSTOMER_ACCOUNT left outer join
			HOLDING_SUMMARY on HS_CA_ID = CA_ID,
			LAST_TRADE
		WHERE	CA_C_ID = customer_id AND
			LT_S_SYMB = HS_S_SYMB
		GROUP BY CA_ID, CA_BAL
		ORDER BY 3 asc
		LIMIT max_acct_len
	LOOP
		acct_id = acct_id || '|' || rs.CA_ID;
		acct_bal = acct_bal || '|' || rs.CA_BAL;

		IF rs.soma is null THEN
			asset_total = asset_total || '|' || 0;
		ELSE
			asset_total = asset_total || '|' || rs.soma;
		END IF;
		i = i + 1;
	END LOOP;

	acct_len = i - 1;

	SELECT	customer_id,
		acct_id,
		acct_len,
		acct_bal,
		asset_total,
		C_ST_ID,
		C_L_NAME,
		C_F_NAME,
		C_M_NAME,
		C_GNDR,
		C_TIER,
		extract(year from C_DOB),
		extract(month from C_DOB),
		extract(day from C_DOB),
		extract(hour from C_DOB),
		extract(minute from C_DOB),
		extract(second from C_DOB),
		C_AD_ID,
		C_CTRY_1,
		C_AREA_1,
		C_LOCAL_1,
		C_EXT_1,
		C_CTRY_2,
		C_AREA_2,
		C_LOCAL_2,
		C_EXT_2,
		C_CTRY_3,
		C_AREA_3,
		C_LOCAL_3,
		C_EXT_3,
		C_EMAIL_1,
		C_EMAIL_2
	INTO	rs
	FROM	CUSTOMER
	WHERE	C_ID = customer_id;

	RETURN NEXT rs;

END;
$$ LANGUAGE 'plpgsql';


/*
 * Frame 2
 * returns information for the first N (max_trades) trades executed for the
 * specified customer account at or after the specified time.
 */

CREATE OR REPLACE FUNCTION CustomerPositionFrame2(IN acct_id IDENT_T) RETURNS SETOF record AS $$
DECLARE
	-- variables
	rs		RECORD;
BEGIN
	-- Should return 1 to 30 rows.

	FOR rs IN
		SELECT	T_ID,
			T_S_SYMB,
			T_QTY,
			ST_NAME,
			extract(year from TH_DTS),
			extract(month from TH_DTS),
			extract(day from TH_DTS),
			extract(hour from TH_DTS),
			extract(minute from TH_DTS),
			extract(second from TH_DTS)
		FROM	(SELECT
			T_ID as ID
			FROM	TRADE
			WHERE	T_CA_ID = acct_id
			ORDER BY T_DTS desc LIMIT 10) as T,
			TRADE,
			TRADE_HISTORY,
			STATUS_TYPE
		WHERE	T_ID = ID AND
			TH_T_ID = T_ID AND
			ST_ID = TH_ST_ID
		ORDER BY TH_DTS desc
		LIMIT 30
	LOOP
		RETURN NEXT rs;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';

