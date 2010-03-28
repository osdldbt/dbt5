/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 15 July 2006
 */

#include "SecurityDetailDB.h"

// Call Security Detail Frame 1
void CSecurityDetailDB::DoSecurityDetailFrame1(
		const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< SDF1" << endl;
	cout << "- Security Detail Frame 1 (input)" << endl <<
			"-- access_lob_flag: " << pIn->access_lob_flag << endl <<
			"-- start_day: " << pIn->start_day.year << "-" <<
					pIn->start_day.month << "-" << pIn->start_day.day << " " <<
					pIn->start_day.hour << ":" << pIn->start_day.minute <<
					":" << pIn->start_day.second << endl <<
			"-- max_rows_to_return: " << pIn->max_rows_to_return << endl <<
			"-- symbol: "<<pIn->symbol<<endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Security Detail Frame 1 (output)" << endl <<
			"-- fin_len: " << pOut->fin_len << endl <<
			"-- day_len: " << pOut->day_len << endl <<
			"-- news_len: " << pOut->news_len << endl;
	int i;
	for (i = 0; i < 3; i++) {
		cout << "-- cp_co_name[" << i << "]: " << pOut->cp_co_name[i] << endl <<
			"-- cp_in_name[" << i << "]: " << pOut->cp_in_name[i] << endl;
	}
	for (i = 0; i < pOut->fin_len; i++) {
		cout << "-- fin[" << i << "].year: " << pOut->fin[i].year << endl <<
				"-- fin[" << i << "].qtr: " << pOut->fin[i].qtr << endl <<
				"-- fin[" << i << "].start_date: " <<
						pOut->fin[i].start_date.year << "-" <<
						pOut->fin[i].start_date.month << "-" <<
						pOut->fin[i].start_date.day << endl <<
				"-- fin[" << i << "].rev: "<<pOut->fin[i].rev << endl <<
				"-- fin[" << i << "].net_earn: " << pOut->fin[i].net_earn <<
						endl <<
				"-- fin[" << i << "].basic_eps: " << pOut->fin[i].basic_eps <<
						endl <<
				"-- fin[" << i << "].dilut_eps: " << pOut->fin[i].dilut_eps <<
						endl <<
				"-- fin[" << i << "].margin: " << pOut->fin[i].margin << endl <<
				"-- fin[" << i << "].invent: " << pOut->fin[i].invent << endl <<
				"-- fin[" << i << "].assets: " << pOut->fin[i].assets << endl <<
				"-- fin[" << i << "].liab: " << pOut->fin[i].liab << endl <<
				"-- fin[" << i << "].out_basic: " << pOut->fin[i].out_basic <<
						endl <<
				"-- fin[" << i << "].out_dilut: " << pOut->fin[i].out_dilut <<
						endl;
	}
	for (i = 0; i < pOut->day_len; i++) {
		cout << "-- day[" << i << "].date: " <<
					pOut->day[i].date.year << "-" <<
					pOut->day[i].date.month << "-" <<
					pOut->day[i].date.day << endl <<
			"-- day[" << i << "].close: " << pOut->day[i].close << endl <<
			"-- day[" << i << "].high: " << pOut->day[i].high << endl <<
			"-- day[" << i << "].low: " << pOut->day[i].low << endl <<
			"-- day[" << i << "].vol: " << pOut->day[i].vol << endl;
	}
	for (i = 0; i < pOut->news_len; i++) {
		cout << "-- news[" << i << "].item: " << pOut->news[i].item << endl <<
				"-- news[" << i << "].dts: " <<
						pOut->news[i].dts.year << "-" <<
						pOut->news[i].dts.month << "-" <<
						pOut->news[i].dts.day << " " <<
						pOut->news[i].dts.hour << ":" <<
						pOut->news[i].dts.minute << ":" <<
						pOut->news[i].dts.second << endl <<
				"-- news[" << i << "].src: " << pOut->news[i].src << endl <<
				"-- news[" << i << "].auth: " << pOut->news[i].auth << endl <<
				"-- news[" << i << "].headline: " << pOut->news[i].headline <<
						endl <<
				"-- news[" << i << "].summary: " << pOut->news[i].summary <<
						endl;
	}
	cout << "-- last_price: " << pOut->last_price << endl <<
			"-- last_open: " << pOut->last_open << endl <<
			"-- last_vol: " << pOut->last_vol << endl <<
			"-- s_name: " << pOut->s_name << endl <<
			"-- co_name: " << pOut->co_name << endl <<
			"-- sp_rate: " << pOut->sp_rate << endl <<
			"-- ceo_name: " << pOut->ceo_name << endl <<
			"-- co_desc: " << pOut->co_desc << endl <<
			"-- open_date: " << pOut->open_date.year << "-" <<
					pOut->open_date.month << "-" <<
					pOut->open_date.day << " " <<
					pOut->open_date.hour << ":" <<
					pOut->open_date.minute << ":" <<
					pOut->open_date.second << endl <<
			"-- co_st_id: " << pOut->co_st_id << endl <<
			"-- co_ad_line1: " << pOut->co_ad_line1 << endl <<
			"-- co_ad_line2: " << pOut->co_ad_line2 << endl <<
			"-- co_ad_town: " << pOut->co_ad_town << endl <<
			"-- co_ad_div: " << pOut->co_ad_div << endl <<
			"-- co_ad_zip: " << pOut->co_ad_zip << endl <<
			"-- co_ad_cty: " << pOut->co_ad_cty << endl <<
			"-- num_out: " << pOut->num_out << endl <<
			"-- start_date: " << pOut->start_date.year << "-" <<
					pOut->start_date.month << "-" <<
					pOut->start_date.day << " " <<
					pOut->start_date.hour << ":" <<
					pOut->start_date.minute << ":" <<
					pOut->start_date.second << endl <<
			"-- ex_date: " << pOut->ex_date.year << "-" <<
					pOut->ex_date.month << "-" <<
					pOut->ex_date.day << " " << pOut->ex_date.hour <<
					":" << pOut->ex_date.minute << ":" <<
					pOut->ex_date.second << endl <<
			"-- pe_ratio: " << pOut->pe_ratio << endl <<
			"-- s52_wk_high: " << pOut->s52_wk_high << endl <<
			"-- s52_wk_high_date: " << pOut->s52_wk_high_date.year <<
					"-" << pOut->s52_wk_high_date.month << "-" <<
					pOut->s52_wk_high_date.day << " " <<
					pOut->s52_wk_high_date.hour << ":" <<
					pOut->s52_wk_high_date.minute << ":" <<
					pOut->s52_wk_high_date.second << endl <<
			"-- s52_wk_low: " << pOut->s52_wk_low << endl <<
			"-- s52_wk_low_date: " << pOut->s52_wk_low_date.year <<
					"-" << pOut->s52_wk_low_date.month << "-" <<
					pOut->s52_wk_low_date.day << " " <<
					pOut->s52_wk_low_date.hour << ":" <<
					pOut->s52_wk_low_date.minute << ":" <<
					pOut->s52_wk_low_date.second << endl <<
			"-- divid: " << pOut->divid << endl <<
			"-- yield: " << pOut->yield << endl <<
			"-- ex_ad_div: " << pOut->ex_ad_div << endl <<
			"-- ex_ad_cty: " << pOut->ex_ad_cty << endl <<
			"-- ex_ad_line1: " << pOut->ex_ad_line1 << endl <<
			"-- ex_ad_line2: " << pOut->ex_ad_line2 << endl <<
			"-- ex_ad_town: " << pOut->ex_ad_town << endl <<
			"-- ex_ad_zip: " << pOut->ex_ad_zip << endl <<
			"-- ex_close: " << pOut->ex_close << endl <<
			"-- ex_desc: " << pOut->ex_desc << endl <<
			"-- ex_name: " << pOut->ex_name << endl <<
			"-- ex_num_symb: " << pOut->ex_num_symb << endl <<
			"-- ex_open: " << pOut->ex_open << endl;
	cout << ">>> SDF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
