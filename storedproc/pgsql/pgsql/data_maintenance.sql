-- This file is released under the terms of the Artistic License.  Please see
-- the file LICENSE, included in this package, for details.
--
-- Copyright The DBT-5 Authors
--
-- Based on TPC-E Standard Specification Revision 1.14.0.
-- Clause 3.3.11.3
CREATE OR REPLACE FUNCTION DataMaintenanceFrame1 (
    IN acct_id IDENT_T
  , IN c_id IDENT_T
  , IN co_id IDENT_T
  , IN day_of_month INTEGER
  , IN symbol VARCHAR(15)
  , IN table_name VARCHAR(18)
  , IN tx_id VARCHAR(4)
  , IN vol_incr INTEGER
) RETURNS INTEGER
AS $$
DECLARE
    -- variables
    rowcount INTEGER;
    acl VARCHAR(4);
    line2 VARCHAR;
    addr_id IDENT_T;
    sprate CHAR(4);
    email2 VARCHAR;
    len INTEGER;
    lenMindspring INTEGER;
    old_tax_rate VARCHAR(3);
    new_tax_rate VARCHAR(3);
    tax_name VARCHAR(50);
    pos INTEGER;
    old_symbol VARCHAR(15);
    new_symbol VARCHAR(15);
BEGIN
    IF table_name = 'ACCOUNT_PERMISSION' THEN
        -- ACCOUNT_PERMISSION
        -- Update the AP_ACL to “1111” or “0011” in rows for
        -- an account of acct_id.
        acl = NULL;
        SELECT ap_acl
        INTO acl
        FROM account_permission
        WHERE ap_ca_id = acct_id
        ORDER BY ap_acl DESC
        LIMIT 1;
        IF acl != '1111' THEN
            UPDATE account_permission
            SET ap_acl = '1111'
            WHERE ap_ca_id = acct_id
              AND ap_acl = acl;
        ELSE
            -- ACL is “1111” change it to '0011'
            UPDATE account_permission
            SET ap_acl = '0011'
            WHERE ap_ca_id = acct_id
              AND ap_acl = acl;
        END IF;
    ELSIF table_name = 'ADDRESS' THEN
        -- ADDRESS
        -- Change AD_LINE2 in the ADDRESS table for
        -- the CUSTOMER with C_ID of c_id.
        line2 = NULL;
        addr_id = 0;
        IF DataMaintenanceFrame1.c_id != 0 THEN
            SELECT ad_line2
                 , ad_id
            INTO line2
               , addr_id
            FROM address
               , customer
            WHERE ad_id = c_ad_id
              AND customer.c_id = DataMaintenanceFrame1.c_id;
        ELSE
            SELECT ad_line2
                 , ad_id
            INTO line2
               , addr_id
            FROM address
               , company
            WHERE ad_id = co_ad_id
              AND company.co_id = DataMaintenanceFrame1.co_id;
        END IF;
        IF line2 != 'Apt. 10C' THEN
            UPDATE address
            SET ad_line2 = 'Apt. 10C'
            WHERE ad_id = addr_id;
        ELSE
            UPDATE address
            SET ad_line2 = 'Apt. 22'
            WHERE ad_id = addr_id;
        END IF;
    ELSIF table_name = 'COMPANY' THEN
        -- COMPANY
        -- Update a row in the COMPANY table identified
        -- by co_id, set the company’s Standard and Poor
        -- credit rating to “ABA” or to “AAA”.
        sprate = NULL;
        SELECT co_sp_rate
          INTO sprate
        FROM company
        WHERE company.co_id = DataMaintenanceFrame1.co_id;
        IF sprate != 'ABA' THEN
            UPDATE company
            SET co_sp_rate = 'ABA'
            WHERE company.co_id = DataMaintenanceFrame1.co_id;
        ELSE
            UPDATE company
            SET co_sp_rate = 'AAA'
            WHERE company.co_id = DataMaintenanceFrame1.co_id;
        END IF;
    ELSIF table_name = 'CUSTOMER' THEN
        -- CUSTOMER
        -- Update the second email address of a CUSTOMER
        -- identified by c_id. Set the ISP part of the customer’s
        -- second email address to “@mindspring.com”
        -- or “@earthlink.com”.
        email2 = NULL;
        len = 0;
        lenMindspring = char_length('@mindspring.com');
        SELECT c_email_2
        INTO email2
        FROM customer
        WHERE customer.c_id = DataMaintenanceFrame1.c_id;
        len = char_length(email2);
        len = len - lenMindspring;
        IF len > 0
                AND substring(email2 FROM len + 1 FOR lenMindspring) = '@mindspring.com'
        THEN
            UPDATE customer
            SET c_email_2 = substring(c_email_2 FROM 1 FOR position('@' IN c_email_2)) || 'earthlink.com'
            WHERE customer.c_id = DataMaintenanceFrame1.c_id;
        ELSE
            -- set to @mindspring.com
            UPDATE customer
            SET c_email_2 = substring(c_email_2 FROM 1 FOR position('@' IN c_email_2)) || 'mindspring.com'
            WHERE customer.c_id = DataMaintenanceFrame1.c_id;
        END IF;
    ELSIF table_name = 'CUSTOMER_TAXRATE' THEN
        -- CUSTOMER_TAXRATE
        -- A tax rate identified by “999” will be inserted into
        -- the CUSTOMER_TAXRATE table for the CUSTOMER identified
        -- by c_id.If the customer already has the “999” tax
        -- rate, the tax Rate will be deleted. To preserve for
        -- foreign key integrity The “999” tax rate must exist
        -- in the TAXRATE table.
        rowcount = 0;
        SELECT cx_tx_id
        INTO old_tax_rate
        FROM customer_taxrate
        WHERE cx_c_id = DataMaintenanceFrame1.c_id
          AND (
                  cx_tx_id LIKE 'US%'
               OR cx_tx_id LIKE 'CN%'
              );
        IF (substring(old_tax_rate FROM 1 FOR 2) = 'US') THEN
            IF (old_tax_rate = 'US5') THEN
                new_tax_rate := 'US1';
            ELSIF (old_tax_rate = 'US4') THEN
                new_tax_rate := 'US5';
            ELSIF (old_tax_rate = 'US3') THEN
                new_tax_rate := 'US4';
            ELSIF (old_tax_rate = 'US2') THEN
                new_tax_rate := 'US3';
            ELSE
                new_tax_rate := 'US2';
            END IF;
        ELSE
            IF (old_tax_rate = 'CN4') THEN
                new_tax_rate := 'CN1';
            ELSIF (old_tax_rate = 'CN3') THEN
                new_tax_rate := 'CN4';
            ELSIF (old_tax_rate = 'CN2') THEN
                new_tax_rate := 'CN3';
            ELSE
                new_tax_rate := 'CN2';
            END IF;
        END IF;
        UPDATE customer_taxrate
        SET cx_tx_id = new_tax_rate
        WHERE cx_c_id = DataMaintenanceFrame1.c_id
          AND cx_tx_id = old_tax_rate;
    ELSIF table_name = 'DAILY_MARKET' THEN
        --- DAILY_MARKET
        UPDATE daily_market
        SET dm_vol = dm_vol + vol_incr
        WHERE dm_s_symb = symbol
          AND substring(dm_date::TEXT FROM 6 FOR 2)::INTEGER = day_of_month;
    ELSIF table_name = 'EXCHANGE' THEN
        --- EXCHANGE
        -- - Other than the table_name, no additional
        -- - parameters are used when the table_name is EXCHANGE.
        -- - There are only four rows in the EXCHANGE table. Every
        -- - row will have its EX_DESC updated. If EX_DESC does not
        -- - already end with “LAST UPDATED “ and a date and time,
        -- - that string will be appended to EX_DESC. Otherwise the
        -- - date and time at the end of EX_DESC will be updated
        -- - to the current date and time.
        rowcount = 0;
        SELECT count(*)
        INTO rowcount
        FROM exchange
        WHERE ex_desc LIKE '%LAST UPDATED%';
        IF rowcount = 0 THEN
            UPDATE exchange
            SET ex_desc = ex_desc || ' LAST UPDATED ' || now();
        ELSE
            UPDATE exchange
	        SET ex_desc = substring(ex_desc FROM 1 FOR char_length(ex_desc) - char_length(now()::TEXT)) || now();
        END IF;
    ELSIF table_name = 'FINANCIAL' THEN
        -- FINANCIAL
        -- Update the FINANCIAL table for a company identified by
        -- co_id. That company’s FI_QTR_START_DATEs will be
        -- updated to the second of the month or to the first of
        -- the month if the dates were already the second of the
        -- month.
        rowcount = 0;
        SELECT count(*)
        INTO rowcount
        FROM financial
        WHERE fi_co_id = DataMaintenanceFrame1.co_id
          AND substring(fi_qtr_start_date::TEXT FROM 9 FOR 2)::SMALLINT = 1;
        IF rowcount > 0 THEN
            UPDATE financial
            SET fi_qtr_start_date = fi_qtr_start_date + INTERVAL '1 DAY'
            WHERE fi_co_id = DataMaintenanceFrame1.co_id;
        ELSE
            UPDATE financial
            SET fi_qtr_start_date = fi_qtr_start_date - INTERVAL '1 DAY'
            WHERE fi_co_id = DataMaintenanceFrame1.co_id;
        END IF;
    ELSIF table_name = 'NEWS_ITEM' THEN
        -- NEWS_ITEM
        -- Update the news items for a specified company.
        -- Change the NI_DTS by 1 day.
        UPDATE news_item
        SET ni_dts = ni_dts + INTERVAL '1 DAY'
        WHERE ni_id = (
                SELECT nx_ni_id
                FROM news_xref
                WHERE nx_co_id = DataMaintenanceFrame1.co_id
                LIMIT 1);
    ELSIF table_name = 'SECURITY' THEN
        -- SECURITY
        -- Update a security identified symbol, increment
        -- S_EXCH_DATE by 1 day.
        UPDATE security
        SET s_exch_date = s_exch_date + INTERVAL '1 DAY'
        WHERE S_SYMB = symbol;
    ELSIF table_name = 'TAXRATE' THEN
        -- TAXRATE
        -- Update a TAXRATE identified by tx_id. The tax rate’s
        -- TX_NAME Will be updated to end with the word “rate”,
        -- or the word“rate” will be removed from the end of the
        -- TX_NAME if TX_NAME already ends with the word “rate”.
        tax_name := NULL;
        pos := 0;
        -- changed from 0 to 1
        SELECT tx_name
        INTO tax_name
        FROM taxrate
        WHERE taxrate.tx_id = DataMaintenanceFrame1.tx_id;
        pos = position(' Tax ' IN tax_name);
        IF (pos != 0) THEN
            tax_name := overlay(' Tax ' PLACING 't' FROM 2 FOR 1);
        ELSE
            tax_name := overlay(' tax ' PLACING 'T' FROM 2 FOR 1);
        END IF;
        UPDATE taxrate
        SET tx_name = tax_name
        WHERE taxrate.tx_id = DataMaintenanceFrame1.tx_id;
    ELSIF table_name = 'WATCH_ITEM' THEN
        SELECT count(*)
        INTO rowcount
        FROM watch_item
           , watch_list
        WHERE wl_c_id = DataMaintenanceFrame1.c_id
          AND wi_wl_id = wl_id;
        rowcount := (rowcount + 1) / 2;
        SELECT wi_s_symb
        INTO old_symbol
        FROM (
                 SELECT wi_s_symb
                 FROM watch_item
                    , watch_list
                 WHERE wl_c_id = DataMaintenanceFrame1.c_id
                   AND wi_wl_id = wl_id
                 ORDER BY wi_s_symb ASC
             ) AS something
        OFFSET rowcount
        LIMIT 1;
        SELECT s_symb
        INTO new_symbol
        FROM security
        WHERE s_symb > old_symbol
          AND s_symb NOT IN (
                                SELECT wi_s_symb
                                FROM watch_item
                                   , watch_list
                                WHERE wl_c_id = DataMaintenanceFrame1.c_id
                                  AND wi_wl_id = wl_id
                            )
        ORDER BY s_symb ASC
        LIMIT 1;
        UPDATE watch_item
        SET wi_s_symb = new_symbol
        FROM watch_list
        WHERE wl_c_id = DataMaintenanceFrame1.c_id
          AND wi_wl_id = wl_id
          AND wi_s_symb = old_symbol;
    END IF;
    RETURN 0;
END;
$$
LANGUAGE 'plpgsql';
