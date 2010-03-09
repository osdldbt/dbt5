/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Security Detail transaction
 * -------------------------
 * This transaction returns all available information related to a given
 * security.
 * 
 *
 * Based on TPC-E Standard Specification Draft Revision 0.32.2e Clause 3.3.8.
 */

/*
 * Frame 1
 * Get all details about the security
 */

CREATE OR REPLACE FUNCTION SecurityDetailFrame1 (
						IN access_lob_flag	smallint,
						IN max_rows_to_return	integer,
						IN start_day		timestamp,
						IN symbol		char(15)) RETURNS record AS $$
DECLARE
	-- output parameters
	cp_co_name	text;
	cp_in_name	text;
	fin_year	text;
	fin_qtr		text;
	fin_start_year	text;
	fin_start_month	text;
	fin_start_day	text;
	fin_start_hour	text;
	fin_start_min	text;
	fin_start_sec	text;
	fin_rev		text;
	fin_net_earn	text;
	fin_basic_eps	text;
	fin_dilut_eps	text;
	fin_margin	text;
	fin_invent	text;
	fin_assets	text;
	fin_liab	text;
	fin_out_basic	text;
	fin_out_dilut	text;
	day_date_year	text;
	day_date_month	text;
	day_date_day	text;
	day_date_hour	text;
	day_date_minute	text;
	day_date_second	text;
	day_close	text;
	day_high	text;
	day_low		text;
	day_vol		text;
	news_it		text;
	news_year	text;
	news_month	text;
	news_day	text;
	news_hour	text;
	news_minute	text;
	news_second	text;
	news_src	text;
	news_auth	text;
	news_headline	text;
	news_summary	text;
	last_price	S_PRICE_T;
	last_open	S_PRICE_T;
	last_vol	S_COUNT_T;
	fin_len		integer;
	day_len		integer;
	news_len	integer;

	-- variables
	comp_id		IDENT_T;
	rs		RECORD;
	i		integer;
BEGIN
	-- initialize text variables
	cp_co_name = '';
	cp_in_name = '';
	fin_year = '';
	fin_qtr = '';
	fin_start_year = '';
	fin_start_month = '';
	fin_start_day = '';
	fin_start_hour = '';
	fin_start_min = '';
	fin_start_sec = '';
	fin_rev = '';
	fin_net_earn = '';
	fin_basic_eps = '';
	fin_dilut_eps = '';
	fin_margin = '';
	fin_invent = '';
	fin_assets = '';
	fin_liab = '';
	fin_out_basic = '';
	fin_out_dilut = '';
	day_date_year = '';
	day_date_month = '';
	day_date_day = '';
	day_date_hour = '';
	day_date_minute = '';
	day_date_second = '';
	day_close = '';
	day_high = '';
	day_low	 = '';
	day_vol	 = '';
	news_it	 = '';
	news_year = '';
	news_month = '';
	news_day = '';
	news_hour = '';
	news_minute = '';
	news_second = '';
	news_src = '';
	news_auth = '';
	news_headline = '';
	news_summary = '';

	-- get company id from symbol

	SELECT	CO_ID
	INTO	comp_id
	FROM	SECURITY,
		COMPANY
	WHERE	S_SYMB = symbol AND
		S_CO_ID = CO_ID;

	-- Should return max_comp_len rows

	FOR rs IN
		SELECT	CO_NAME,
			IN_NAME
		FROM	COMPANY_COMPETITOR,
			COMPANY,
			INDUSTRY
		WHERE	CP_CO_ID = comp_id AND
			CO_ID = CP_COMP_CO_ID AND
			IN_ID = CP_IN_ID
		LIMIT 3
	LOOP
		cp_co_name = cp_co_name || '|' || rs.CO_NAME;
		cp_in_name = cp_in_name || '|' || rs.IN_NAME;
	END LOOP;

	-- Should return max_fin_len rows
	
	i = 0;
	FOR rs IN
		SELECT	FI_YEAR,
			FI_QTR,
			extract(year from FI_QTR_START_DATE) as year,
			extract(month from FI_QTR_START_DATE) as month,
			extract(day from FI_QTR_START_DATE) as day,
			extract(hour from FI_QTR_START_DATE) as hour,
			extract(minute from FI_QTR_START_DATE) as minute,
			extract(second from FI_QTR_START_DATE) as second,
			FI_REVENUE,
			FI_NET_EARN,
			FI_BASIC_EPS,
			FI_DILUT_EPS,
			FI_MARGIN,
			FI_INVENTORY,
			FI_ASSETS,
			FI_LIABILITY,
			FI_OUT_BASIC,
			FI_OUT_DILUT
		FROM	FINANCIAL
		WHERE	FI_CO_ID = comp_id
		ORDER BY FI_YEAR asc, FI_QTR
		LIMIT 20
	LOOP
		fin_year = fin_year || '|' || rs.FI_YEAR;
		fin_qtr = fin_qtr || '|' || rs.FI_QTR;
		fin_start_year = fin_start_year || '|' || rs.year;
		fin_start_month = fin_start_month || '|' || rs.month;
		fin_start_day = fin_start_day || '|' || rs.day;
		fin_start_hour = fin_start_hour || '|' || rs.hour;
		fin_start_min = fin_start_min || '|' || rs.minute;
		fin_start_sec = fin_start_sec || '|' || rs.second;
		fin_rev = fin_rev || '|' || rs.FI_REVENUE;
		fin_net_earn = fin_net_earn || '|' || rs.FI_NET_EARN;
		fin_basic_eps = fin_basic_eps || '|' || rs.FI_BASIC_EPS;
		fin_dilut_eps = fin_dilut_eps || '|' || rs.FI_DILUT_EPS;
		fin_margin = fin_margin || '|' || rs.FI_MARGIN;
		fin_invent = fin_invent || '|' || rs.FI_INVENTORY;
		fin_assets = fin_assets || '|' || rs.FI_ASSETS;
		fin_liab = fin_liab || '|' || rs.FI_LIABILITY;
		fin_out_basic = fin_out_basic || '|' || rs.FI_OUT_BASIC;
		fin_out_dilut = fin_out_dilut || '|' || rs.FI_OUT_DILUT;
		i = i + 1;
	END LOOP;

	fin_len = i;

	-- Should return max_rows_to_return rows
	
	i = 0;
	FOR rs IN
		SELECT	extract(year from DM_DATE) as year,
			extract(month from DM_DATE) as month,
			extract(day from DM_DATE) as day,
			extract(hour from DM_DATE) as hour,
			extract(minute from DM_DATE) as minute,
			extract(second from DM_DATE) as second,
			DM_CLOSE,
			DM_HIGH,
			DM_LOW,
			DM_VOL
		FROM	DAILY_MARKET
		WHERE	DM_S_SYMB = symbol AND
			DM_DATE >= start_day
		ORDER BY DM_DATE asc
		LIMIT max_rows_to_return
	LOOP
		day_date_year = day_date_year || '|' || rs.year;
		day_date_month = day_date_month || '|' || rs.month;
		day_date_day = day_date_day || '|' || rs.day;
		day_date_hour = day_date_hour || '|' || rs.hour;
		day_date_minute = day_date_minute || '|' || rs.minute;
		day_date_second = day_date_second || '|' || rs.second;
		day_close = day_close || '|' || rs.DM_CLOSE;
		day_high = day_high || '|' || rs.DM_HIGH;
		day_low = day_low || '|' || rs.DM_LOW;
		day_vol = day_vol || '|' || rs.DM_VOL;
		i = i + 1;
	END LOOP;

	day_len = i;

	SELECT	LT_PRICE,
		LT_OPEN_PRICE,
		LT_VOL
	INTO	last_price,
		last_open,
		last_vol
	FROM	LAST_TRADE
	WHERE	LT_S_SYMB = symbol
	LIMIT max_rows_to_return;
	
	-- Should return max_news_len rows
	
	i = 0;
	IF access_lob_flag = 1 THEN
		FOR rs IN
			SELECT	NI_ITEM,
				extract(year from NI_DTS) as year,
				extract(month from NI_DTS) as month,
				extract(day from NI_DTS) as day,
				extract(hour from NI_DTS) as hour,
				extract(minute from NI_DTS) as minute,
				extract(second from NI_DTS) as second,
				NI_SOURCE,
				NI_AUTHOR
			FROM	NEWS_XREF,
				NEWS_ITEM
			WHERE	NI_ID = NX_NI_ID AND
				NX_CO_ID = comp_id
			LIMIT 2
		LOOP
			news_it = news_it || '|' || rs.NI_ITEM;
			news_year = news_year || '|' || rs.year;
			news_month = news_month || '|' || rs.month;
			news_day = news_day || '|' || rs.day;
			news_hour = news_hour || '|' || rs.hour;
			news_minute = news_minute || '|' || rs.minute;
			news_second = news_second || '|' || rs.second;
			news_src = news_src || '|' || rs.NI_SOURCE;
			news_auth = news_auth || '|' || rs.NI_AUTHOR;
			news_headline = news_headline || '|' || '';
			news_summary = news_summary || '|' || '';
			i = i + 1;
		END LOOP;
	ELSE
		FOR rs IN
			SELECT	extract(year from NI_DTS) as year,
				extract(month from NI_DTS) as month,
				extract(day from NI_DTS) as day,
				extract(hour from NI_DTS) as hour,
				extract(minute from NI_DTS) as minute,
				extract(second from NI_DTS) as second,
				NI_SOURCE,
				NI_AUTHOR,
				NI_HEADLINE,
				NI_SUMMARY
			FROM	NEWS_XREF,
				NEWS_ITEM
			WHERE	NI_ID = NX_NI_ID AND
				NX_CO_ID = comp_id
			LIMIT 2
		LOOP
			news_it = news_it || '|' || '';
			news_year = news_year || '|' || rs.year;
			news_month = news_month || '|' || rs.month;
			news_day = news_day || '|' || rs.day;
			news_hour = news_hour || '|' || rs.hour;
			news_minute = news_minute || '|' || rs.minute;
			news_second = news_second || '|' || rs.second;
			news_src = news_src || '|' || rs.NI_SOURCE;
			news_auth = news_auth || '|' || rs.NI_AUTHOR;
			news_headline = news_headline || '|' || rs.NI_HEADLINE;
			news_summary = news_summary || '|' || rs.NI_SUMMARY;
			i = i + 1;
		END LOOP;
	END IF;
	
	news_len = i;

	SELECT	fin_len,
		day_len,
		news_len,
		cp_co_name,
		cp_in_name,
		fin_year,
		fin_qtr,
		fin_start_year,
		fin_start_month,
		fin_start_day,
		fin_start_hour,
		fin_start_min,
		fin_start_sec,
		fin_rev,
		fin_net_earn,
		fin_basic_eps,
		fin_dilut_eps,
		fin_margin,
		fin_invent,
		fin_assets,
		fin_liab,
		fin_out_basic,
		fin_out_dilut,
		day_date_year,
		day_date_month,
		day_date_day,
		day_date_hour,
		day_date_minute,
		day_date_second,
		day_close,
		day_high,
		day_low,
		day_vol,
		news_it,
		news_year,
		news_month,
		news_day,
		news_hour,
		news_minute,
		news_second,
		news_src,
		news_auth,
		news_headline,
		news_summary,
		last_price,
		last_open,
		last_vol,
		S_NAME,
		CO_ID,
		CO_NAME,
		CO_SP_RATE,
		CO_CEO,
		CO_DESC,
		extract(year from CO_OPEN_DATE) as year,
		extract(month from CO_OPEN_DATE) as month,
		extract(day from CO_OPEN_DATE) as day,
		extract(hour from CO_OPEN_DATE) as hour,
		extract(minute from CO_OPEN_DATE) as minute,
		extract(second from CO_OPEN_DATE) as second,
		CO_ST_ID,
		CA.AD_LINE1,
		CA.AD_LINE2,
		ZCA.ZC_TOWN,
		ZCA.ZC_DIV,
		CA.AD_ZC_CODE,
		CA.AD_CTRY,
		S_NUM_OUT,
		extract(year from S_START_DATE) as year,
		extract(month from S_START_DATE) as month,
		extract(day from S_START_DATE) as day,
		extract(hour from S_START_DATE) as hour,
		extract(minute from S_START_DATE) as minute,
		extract(second from S_START_DATE) as second,
		extract(year from S_EXCH_DATE) as year,
		extract(month from S_EXCH_DATE) as month,
		extract(day from S_EXCH_DATE) as day,
		extract(hour from S_EXCH_DATE) as hour,
		extract(minute from S_EXCH_DATE) as minute,
		extract(second from S_EXCH_DATE) as second,
		S_PE,
		S_52WK_HIGH,
		extract(year from S_52WK_HIGH_DATE) as year,
		extract(month from S_52WK_HIGH_DATE) as month,
		extract(day from S_52WK_HIGH_DATE) as day,
		extract(hour from S_52WK_HIGH_DATE) as hour,
		extract(minute from S_52WK_HIGH_DATE) as minute,
		extract(second from S_52WK_HIGH_DATE) as second,
		S_52WK_LOW,
		extract(year from S_52WK_LOW_DATE) as year,
		extract(month from S_52WK_LOW_DATE) as month,
		extract(day from S_52WK_LOW_DATE) as day,
		extract(hour from S_52WK_LOW_DATE) as hour,
		extract(minute from S_52WK_LOW_DATE) as minute,
		extract(second from S_52WK_LOW_DATE) as second,
		S_DIVIDEND,
		S_YIELD,
		ZEA.ZC_DIV,
		EA.AD_CTRY,
		EA.AD_LINE1,
		EA.AD_LINE2,
		ZEA.ZC_TOWN,
		EA.AD_ZC_CODE,
		EX_CLOSE,
		EX_DESC,
		EX_NAME,
		EX_NUM_SYMB,
		EX_OPEN
	INTO	rs
	FROM	SECURITY,
		COMPANY,
		ADDRESS CA,
		ADDRESS EA,
		ZIP_CODE ZCA,
		ZIP_CODE ZEA,
		EXCHANGE
	WHERE	S_SYMB = symbol AND
		CO_ID = S_CO_ID AND
		CA.AD_ID = CO_AD_ID AND
		EA.AD_ID = EX_AD_ID AND
		EX_ID = S_EX_ID AND
		ca.ad_zc_code = zca.zc_code AND
		ea.ad_zc_code = zea.zc_code;

	RETURN rs;
END;
$$ LANGUAGE 'plpgsql';
