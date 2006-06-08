/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Contains structure definitions for all transactions.
*/
#ifndef TXN_HARNESS_STRUCTS_H
#define TXN_HARNESS_STRUCTS_H

#include "EGenStandardTypes.h"
#include "DateTime.h"
#include "MiscConsts.h"
#include "TableConsts.h"
#include "MEETradeRequestActions.h"

namespace TPCE
{

//declare the < operator for timestamps
bool operator< (const TIMESTAMP_STRUCT& ts1, const TIMESTAMP_STRUCT& ts2);

const INT32 iFinYears = 5;
const INT32 iFinQtrPerYear = 4;
const INT32 iMaxDailyHistory = 10;
const INT32 iMaxNews = 10;

const INT32 max_list_len = 100;
const INT32 max_feed_len = 100;
const INT32 max_acct_len = 20;
const INT32 max_hist_len = 30;
const INT32 max_send_len = 40;
const INT32 max_day_len = 20;
const INT32 max_fin_len = 20;
const INT32 max_news_len = 2;
const INT32 max_comp_len = 3;

const INT32 max_table_name = 30;	// for Data Maintenance

const INT32 max_broker_list_len = 8;	// for Broker Volume


/*
*	Broker-Volume
*/
typedef struct TBrokerVolumeTxnInput
{
	// Transaction level inputs
	char			broker_list[max_broker_list_len][cB_NAME_len+1];
	INT32			cust_tier;	
	char			sector_name[cSC_NAME_len+1];	
}	*PBrokerVolumeTxnInput,
	 TBrokerVolumeFrame1Input,	// Single-Frame transaction
	*PBrokerVolumeFrame1Input;	// Single-Frame transaction

typedef struct TBrokerVolumeTxnOutput
{
	// Transaction level outputs
	INT32			list_len;
	INT32			status;
	double			volume[max_broker_list_len];
}	*PBrokerVolumeTxnOutput;

typedef struct TBrokerVolumeFrame1Output
{
	// Frame level outputs
	char			broker_name[max_broker_list_len][cB_NAME_len+1];	
	INT32			list_len;
	INT32			status;
	double			volume[max_broker_list_len];
}	*PBrokerVolumeFrame1Output;
/*
*	Customer-Position
*/
typedef struct TCustomerPositionTxnInput
{
	TIdent		acct_id_idx;	
	TIdent		cust_id;
	bool		get_history;
	char		tax_id[cTAX_ID_len+1];
} *PCustomerPositionTxnInput;

typedef struct TCustomerPositionTxnOutput
{
	TIdent				acct_id[max_acct_len];
	INT32				acct_len;
	double				asset_total[max_acct_len];
	TIdent				c_ad_id;
	char				c_area_1[cAREA_len+1];
	char				c_area_2[cAREA_len+1];
	char				c_area_3[cAREA_len+1];
	char				c_ctry_1[cCTRY_len+1];
	char				c_ctry_2[cCTRY_len+1];
	char				c_ctry_3[cCTRY_len+1];
	TIMESTAMP_STRUCT	c_dob;
	char				c_email_1[cEMAIL_len+1];
	char				c_email_2[cEMAIL_len+1];
	char				c_ext_1[cEXT_len+1];
	char				c_ext_2[cEXT_len+1];
	char				c_ext_3[cEXT_len+1];
	char				c_f_name[cF_NAME_len+1];
	char				c_gndr[cGNDR_len+1];
	char				c_l_name[cL_NAME_len+1];
	char				c_local_1[cLOCAL_len+1];
	char				c_local_2[cLOCAL_len+1];
	char				c_local_3[cLOCAL_len+1];
	char				c_m_name[cM_NAME_len+1];
	char				c_st_id[cST_ID_len+1];
	char				c_tier;
	double				cash_bal[max_acct_len];
	TIMESTAMP_STRUCT	hist_dts[max_hist_len];
	INT32				hist_len;
	INT32				qty[max_hist_len];
	INT32				status;
	char				symbol[max_hist_len][cSYMBOL_len+1];
	TIdent				trade_id[max_hist_len];
	char				trade_status[max_hist_len][cST_NAME_len+1];
} *PCustomerPositionTxnOutput;

typedef struct TCustomerPositionFrame1Input
{
	TIdent				cust_id;
	char				tax_id[cTAX_ID_len+1];
} *PCustomerPositionFrame1Input;

typedef struct TCustomerPositionFrame1Output
{	
	TIdent				acct_id[max_acct_len];	
	INT32				acct_len;
	double				asset_total[max_acct_len];
	TIdent				c_ad_id;
	char				c_area_1[cAREA_len+1];
	char				c_area_2[cAREA_len+1];
	char				c_area_3[cAREA_len+1];
	char				c_ctry_1[cCTRY_len+1];
	char				c_ctry_2[cCTRY_len+1];
	char				c_ctry_3[cCTRY_len+1];
	TIMESTAMP_STRUCT	c_dob;
	char				c_email_1[cEMAIL_len+1];
	char				c_email_2[cEMAIL_len+1];
	char				c_ext_1[cEXT_len+1];
	char				c_ext_2[cEXT_len+1];
	char				c_ext_3[cEXT_len+1];
	char				c_f_name[cF_NAME_len+1];
	char				c_gndr[cGNDR_len+1];
	char				c_l_name[cL_NAME_len+1];
	char				c_local_1[cLOCAL_len+1];
	char				c_local_2[cLOCAL_len+1];
	char				c_local_3[cLOCAL_len+1];
	char				c_m_name[cM_NAME_len+1];
	char				c_st_id[cST_ID_len+1];
	char				c_tier;
	double				cash_bal[max_acct_len];
	TIdent				cust_id;
	INT32				status;


} *PCustomerPositionFrame1Output;

typedef struct TCustomerPositionFrame2Input
{
	TIdent			acct_id;	
} *PCustomerPositionFrame2Input;

typedef struct TCustomerPositionFrame2Output
{	
	TIMESTAMP_STRUCT	hist_dts[max_hist_len];
	INT32				hist_len;
	INT32				qty[max_hist_len];
	INT32				status;
	char				symbol[max_hist_len][cSYMBOL_len+1];
	TIdent				trade_id[max_hist_len];
	char				trade_status[max_hist_len][cST_NAME_len+1];
} *PCustomerPositionFrame2Output;

typedef struct TCustomerPositionFrame3Output
{
	INT32			status;
} *PCustomerPositionFrame3Output;


/*
*	Data-Maintenance
*/
typedef struct TDataMaintenanceTxnInput
{
	INT32	add_flag;
	TIdent	c_id;
	TIdent	co_id;
	INT32	day_of_month;
	char	symbol[cSYMBOL_len+1];
	char	tx_id[cTAX_ID_len+1];
	char	table_name[max_table_name+1];
}	*PDataMaintenanceTxnInput,
	 TDataMaintenanceFrame1Input,	// Single-Frame transaction
	*PDataMaintenanceFrame1Input;	// Single-Frame transaction

typedef struct TDataMaintenanceTxnOutput
{
	INT32	status;
}	*PDataMaintenanceTxnOutput,
	 TDataMaintenanceFrame1Output,	// Single-Frame transaction
	*PDataMaintenanceFrame1Output;	// Single-Frame transaction


/*
*	Market-Feed
*/
// MEE populates this structure
typedef struct TStatusAndTradeType
{	
	char	status_submitted[cST_ID_len+1];
	char	type_limit_buy[cTT_ID_len+1];
	char	type_limit_sell[cTT_ID_len+1];
	char	type_stop_loss[cTT_ID_len+1];	
} *PTStatusAndTradeType;

//Incomming order from SendToMarket interface.
typedef struct TTradeRequest
{
	TIdent					trade_id;
	char					symbol[cSYMBOL_len+1];
	double					price_quote;
	INT32					trade_qty;
	char					trade_type_id[cTT_ID_len+1];
	eMEETradeRequestAction	eAction;
} *PTradeRequest;

//A single entry on the ticker tape feed.
typedef struct TTickerEntry
{
	char				symbol[cSYMBOL_len+1];
	double				price_quote;
	INT32				trade_qty;
} *PTickerEntry;

//Market-Feed data sent from MEE to sponsor provided SUT interface
typedef struct TMarketFeedTxnInput
{
	TStatusAndTradeType	StatusAndTradeType;
	TTickerEntry		Entries[max_feed_len];
}	*PMarketFeedTxnInput,
	 TMarketFeedFrame1Input,	// Single-Frame transaction
	*PMarketFeedFrame1Input;	// Single-Frame transaction

typedef struct TMarketFeedTxnOutput
{
	INT32			send_len;
	INT32			status;
	TIdent			trade_id[max_send_len];
}	*PMarketFeedTxnOutput;

typedef struct TMarketFeedFrame1Output
{
	INT32			send_len;
	INT32			status;
	TTradeRequest	trade_req[max_send_len];
}	*PMarketFeedFrame1Output;


/*
*	Market-Watch
*/
typedef struct TMarketWatchTxnInput
{
	TIdent	acct_id;
	TIdent	c_id;
	TIdent	starting_co_id;
	TIdent	ending_co_id;
	char	industry_name[cIN_NAME_len+1];	
}	*PMarketWatchTxnInput,
	 TMarketWatchFrame1Input,	// Single-Frame transaction
	*PMarketWatchFrame1Input;	// Single-Frame transaction

typedef struct TMarketWatchTxnOutput
{
	double	pct_change;
	INT32	status;
}	*PMarketWatchTxnOutput,
	 TMarketWatchFrame1Output,	// Single-Frame transaction
	*PMarketWatchFrame1Output;	// Single-Frame transaction


/*
*	Security-Detail
*/
typedef struct TFinInfo
{
	INT32				year;
	DB_INDICATOR	year_ind;
	INT32				qtr;
	DB_INDICATOR	qtr_ind;
	TIMESTAMP_STRUCT	start_date;
	DB_INDICATOR	start_date_ind;
	double				rev;
	DB_INDICATOR	rev_ind;
	double				net_earn;
	DB_INDICATOR	net_earn_ind;
	double				basic_eps;
	DB_INDICATOR	basic_eps_ind;
	double				dilut_eps;
	DB_INDICATOR	dilut_eps_ind;
	double				margin;
	DB_INDICATOR	margin_ind;
	double				invent;
	DB_INDICATOR	invent_ind;
	double				assets;
	DB_INDICATOR	assets_ind;
	double				liab;
	DB_INDICATOR	liab_ind;
	double				out_basic;
	DB_INDICATOR	out_basic_ind;
	double				out_dilut;
	DB_INDICATOR	out_dilut_ind;
} *PFinInfo;

typedef struct TDailyHistory
{
	TIMESTAMP_STRUCT	date;
	DB_INDICATOR	date_ind;
	double				close;
	DB_INDICATOR	close_ind;
	double				high;
	DB_INDICATOR	high_ind;
	double				low;
	DB_INDICATOR	low_ind;
	INT64				vol;
	DB_INDICATOR	vol_ind;
} *PDailyHistory;

typedef struct TLastPrice
{
	double	price;
	DB_INDICATOR	price_ind;
	double				open_price;
	DB_INDICATOR	open_price_ind;
	INT64				vol_today;
	DB_INDICATOR	vol_today_ind;
} *PLastPrice;

typedef struct TNews
{
	char				auth[cNI_AUTHOR_len+1];
	DB_INDICATOR	auth_ind;	// Indicator for if the column is NULL
	TIMESTAMP_STRUCT	dts;
	char				headline[cNI_HEADLINE_len+1];
	char				item[cNI_ITEM_len+1];
	char				src[cNI_SOURCE_len+1];
	char				summary[cNI_SUMMARY_len+1];
} *PNews;

typedef struct TSecurityDetailTxnInput
{
	bool				access_lob_flag;
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT    start_day;
	INT32				max_rows_to_return;
}	*PSecurityDetailTxnInput,
	 TSecurityDetailFrame1Input,	// Single-Frame transaction
	*PSecurityDetailFrame1Input;	// Single-Frame transaction

typedef struct TSecurityDetailTxnOutput
{
	INT64				last_vol;
	INT32				news_len;
	INT32				status;
}	*PSecurityDetailTxnOutput;

typedef struct TSecurityDetailFrame1Output
{
	double				s52_wk_high;
	TIMESTAMP_STRUCT	s52_wk_high_date;
	double				s52_wk_low;
	TIMESTAMP_STRUCT	s52_wk_low_date;
	char				ceo_name[cCEO_NAME_len+1];
	char				co_ad_cty[cAD_CTRY_len+1];
	char				co_ad_div[cAD_DIV_len+1];
	char				co_ad_line1[cAD_LINE_len+1];
	char				co_ad_line2[cAD_LINE_len+1];
	char				co_ad_town[cAD_TOWN_len+1];
	char				co_ad_zip[cAD_ZIP_len+1];
	char				co_desc[cCO_DESC_len+1];
	char				co_name[cCO_NAME_len+1];
	char				co_st_id[cST_ID_len+1];
	char				cp_co_name[max_comp_len][cCO_NAME_len+1];
	char				cp_in_name[max_comp_len][cIN_NAME_len+1];
	TDailyHistory		day[max_day_len];
	INT32				day_len;		
	double				divid;
	char				ex_ad_cty[cAD_CTRY_len+1];
	char				ex_ad_div[cAD_DIV_len+1];
	char				ex_ad_line1[cAD_LINE_len+1];
	char				ex_ad_line2[cAD_LINE_len+1];
	char				ex_ad_town[cAD_TOWN_len+1];
	char				ex_ad_zip[cAD_ZIP_len+1];
	INT32				ex_close;
	TIMESTAMP_STRUCT	ex_date;
	char				ex_desc[cEX_DESC_len+1];
	char				ex_name[cEX_NAME_len+1];
	INT32				ex_num_symb;
	INT32				ex_open;
	TFinInfo			fin[max_fin_len];
	INT32				fin_len;
	double				last_open;
	double				last_price;
	INT64				last_vol;
	TNews				news[max_news_len];
	INT32				news_len;
	INT64				num_out;
	TIMESTAMP_STRUCT	open_date;
	double				pe_ratio;
	char				s_name[cS_NAME_len+1];
	char				sp_rate[cSP_RATE_len+1];
	TIMESTAMP_STRUCT	start_date;
	INT32				status;
	double				yield;	
}	*PSecurityDetailFrame1Output;


/*
*	Trade-Lookup
*/
typedef struct TTradeLookupTxnInput
{
	TIdent				acct_id;
	INT32				frame_to_execute;			// which of the frames to execute
	INT32				max_trades;
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT		trade_dts;
	TIdent				trade_id[TradeLookupFrame1MaxRows];
	TIdent				max_acct_id;
} *PTradeLookupTxnInput;
typedef struct TTradeLookupTxnOutput
{
	INT32				frame_executed;				// confirmation of which frame was executed
	bool				is_cash[TradeLookupMaxRows];
	bool				is_market[TradeLookupMaxRows];
	INT32				num_found;
	INT32				status;
	TIdent				trade_list[TradeLookupMaxRows];
} *PTradeLookupTxnOutput;

typedef struct TTradeLookupFrame1Input
{
	INT32				max_trades;
	TIdent				trade_id[TradeLookupFrame1MaxRows];
} *PTradeLookupFrame1Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame1TradeInfo
{
	double				bid_price;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;
	bool				is_market;
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];
	TIMESTAMP_STRUCT	trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
	double				trade_price;

	DB_INDICATOR		trade_id_ind;	
	DB_INDICATOR		bid_price_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;	
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		is_market_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;
	
	DB_INDICATOR		trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_price_ind;
} *PTradeLookupFrame1TradeInfo;

typedef struct TTradeLookupFrame1Output
{
	TTradeLookupFrame1TradeInfo	trade_info[TradeLookupFrame1MaxRows];	
	INT32						num_found;
	INT32						status;	
} *PTradeLookupFrame1Output;

typedef struct TTradeLookupFrame2Input
{
	TIdent				acct_id;
	INT32				max_trades;
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeLookupFrame2Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame2TradeInfo
{
	double				bid_price;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;	
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];	
	TIMESTAMP_STRUCT	trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
	TIdent				trade_id;
	double				trade_price;

	DB_INDICATOR		bid_price_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;	
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;	
	DB_INDICATOR		trade_id_ind;	
	DB_INDICATOR		trade_price_ind;
	DB_INDICATOR		trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
} *PTradeLookupFrame2TradeInfo;

typedef struct TTradeLookupFrame2Output
{	
	TTradeLookupFrame2TradeInfo	trade_info[TradeLookupFrame2MaxRows];
	INT32						num_found;
	INT32						status;	
} *PTradeLookupFrame2Output;

typedef struct TTradeLookupFrame3Input
{
	TIdent				acct_id;
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeLookupFrame3Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame3TradeInfo
{
	TIdent				holding_history_id;
	TIdent				holding_history_trade_id;	
	INT32				quantity_after;
	INT32				quantity_before;

	DB_INDICATOR		holding_history_id_ind;
	DB_INDICATOR		holding_history_trade_id_ind;
	DB_INDICATOR		quantity_before_ind;
	DB_INDICATOR		quantity_after_ind;	
} *PTradeLookupFrame3TradeInfo;

typedef struct TTradeLookupFrame3Output
{
	TTradeLookupFrame3TradeInfo	trade_info[TradeLookupFrame3MaxRows];
	INT32						num_found;
	INT32						status;
	TIdent						trade_id;
} *PTradeLookupFrame3Output;

typedef struct TTradeLookupFrame4Input
{
	INT32				max_trades;
	TIdent				max_acct_id;
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeLookupFrame4Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame4TradeInfo
{
	TIdent				acct_id;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;	
	double				price;
	INT32				quantity;
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];	
	TIMESTAMP_STRUCT	trade_dts;
	TIMESTAMP_STRUCT	trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
	TIdent				trade_id;
	char				trade_type[cTT_ID_len+1];

	DB_INDICATOR		acct_id_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		price_ind;
	DB_INDICATOR		quantity_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;
	DB_INDICATOR		trade_dts_ind;
	DB_INDICATOR		trade_id_ind;
	DB_INDICATOR		trade_type_ind;
	DB_INDICATOR		trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
} *PTradeLookupFrame4TradeInfo;

typedef struct TTradeLookupFrame4Output
{
	TTradeLookupFrame4TradeInfo	trade_info[TradeLookupFrame4MaxRows];	
	INT32						num_found;
	INT32						status;	
} *PTradeLookupFrame4Output;



/*
*	Trade-Order
*/
typedef struct TTradeOrderTxnInput
{
	TIdent			acct_id;
	char			co_name[cCO_NAME_len+1];
	char			exec_f_name[cF_NAME_len+1];
	char			exec_l_name[cL_NAME_len+1];
	char			exec_tax_id[cTAX_ID_len+1];	
	INT32			is_lifo;
	char			issue[cS_ISSUE_len+1];
	double			requested_price;
	INT32			roll_it_back;
	char			st_pending_id[cST_ID_len+1];
	char			st_submitted_id[cST_ID_len+1];
	char			symbol[cSYMBOL_len+1];
	INT32			trade_qty;
	char			trade_type_id[cTT_ID_len+1];
	INT32			type_is_margin;
} *PTradeOrderTxnInput;
typedef struct TTradeOrderTxnOutput
{
	double	buy_value;
	double	sell_value;
	INT32	status;
	double	tax_amount;
	TIdent	trade_id;
} *PTradeOrderTxnOutput;

typedef struct TTradeOrderFrame1Input
{
	TIdent	acct_id;
} *PTradeOrderFrame1Input;

typedef struct TTradeOrderFrame1Output
{	
	char	acct_name[cCA_NAME_len+1];
	char	broker_name[cB_NAME_len+1];
	char	cust_f_name[cF_NAME_len+1];
	TIdent	cust_id;
	char	cust_l_name[cL_NAME_len+1];
	INT32	cust_tier;
	INT32	status;
	char	tax_id[cTAX_ID_len+1];
	INT32	tax_status;
} *PTradeOrderFrame1Output;

typedef struct TTradeOrderFrame2Input
{
	TIdent	acct_id;	
	char	exec_f_name[cF_NAME_len+1];
	char	exec_l_name[cL_NAME_len+1];
	char	exec_tax_id[cTAX_ID_len+1];	
} *PTradeOrderFrame2Input;

typedef struct TTradeOrderFrame2Output
{
	INT32		bad_permission;
	INT32		status;
} *PTradeOrderFrame2Output;

typedef struct TTradeOrderFrame3Input
{
	TIdent	acct_id;
	TIdent	cust_id;
	INT32	cust_tier;
	INT32	is_lifo;
	char	issue[cS_ISSUE_len+1];
	char	st_pending_id[cST_ID_len+1];
	char	st_submitted_id[cST_ID_len+1];
	INT32	tax_status;
	INT32	trade_qty;
	char	trade_type_id[cTT_ID_len+1];
	INT32	type_is_margin;

	// These are IN-OUT parameters.
	char	co_name[cCO_NAME_len+1];
	double	requested_price;
	char	symbol[cSYMBOL_len+1];
} *PTradeOrderFrame3Input;

typedef struct TTradeOrderFrame3Output
{	
	// These are IN-OUT parameters.
	char	co_name[cCO_NAME_len+1];
	double	requested_price;
	char	symbol[cSYMBOL_len+1];

	// These are OUT parameters
	double	buy_value;
	double	charge_amount;
	double	comm_rate;
	double	cust_assets;
	double	market_price;
	char	s_name[cS_NAME_len+1];
	double	sell_value;
	INT32	status;
	char	status_id[cST_ID_len+1];	
	double	tax_amount;
	INT32	type_is_market;
	INT32	type_is_sell;
} *PTradeOrderFrame3Output;

typedef struct TTradeOrderFrame4Input
{
	TIdent	acct_id;
	double	charge_amount;
	double	comm_amount;
	char	exec_name[cEXEC_NAME_len+1];
	INT32	is_cash;
	INT32	is_lifo;
	double	requested_price;
	char	status_id[cST_ID_len+1];
	char	symbol[cSYMBOL_len+1];
	double	tax_amount;
	INT32	trade_qty;
	char	trade_type_id[cTT_ID_len+1];
	INT32	type_is_market;
} *PTradeOrderFrame4Input;

typedef struct TTradeOrderFrame4Output
{	
	INT32	status;
	TIdent	trade_id;
} *PTradeOrderFrame4Output;

typedef struct TTradeOrderFrame5Output
{
	INT32		status;
} *PTradeOrderFrame5Output;

typedef struct TTradeOrderFrame6Output
{
	INT32		status;
} *PTradeOrderFrame6Output;


/*
*	Trade-Result
*/
//Trade-Result data sent from MEE to sponsor provided SUT interface
typedef struct TTradeResultTxnInput
{
	TIdent		trade_id;
	double		trade_price;
} *PTradeResultTxnInput;

typedef struct TTradeResultTxnOutput
{
	double		acct_bal;
	INT32		status;
} *PTradeResultTxnOutput;

typedef struct TTradeResultFrame1Input
{
	TIdent		trade_id;	
} *PTradeResultFrame1Input;

typedef struct TTradeResultFrame1Output
{	
	TIdent	acct_id;
	double	charge;
	INT32	hs_qty;
	INT32	is_lifo;
	INT32	status;
	char	symbol[cSYMBOL_len+1];
	INT32	trade_is_cash;
	INT32	trade_qty;
	char	type_id[cTT_ID_len+1];
	INT32	type_is_market;
	INT32	type_is_sell;
	char	type_name[cTT_NAME_len+1];
} *PTradeResultFrame1Output;

typedef struct TTradeResultFrame2Input
{
	TIdent				acct_id;
	INT32				hs_qty;
	INT32				is_lifo;
	char				symbol[cSYMBOL_len+1];
	TIdent				trade_id;
	double				trade_price;
	INT32				trade_qty;	
	INT32				type_is_sell;
} *PTradeResultFrame2Input;

typedef struct TTradeResultFrame2Output
{
	TIdent				broker_id;
	double				buy_value;
	TIdent				cust_id;
	double				sell_value;
	INT32				status;
	INT32				tax_status;
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeResultFrame2Output;

typedef struct TTradeResultFrame3Input
{
	double	buy_value;	
	TIdent	cust_id;
	double	sell_value;
	TIdent	trade_id;
	double	tax_amount;
} *PTradeResultFrame3Input;

typedef struct TTradeResultFrame3Output
{	
	INT32	status;
	double	tax_amount;
} *PTradeResultFrame3Output;

typedef struct TTradeResultFrame4Input
{
	TIdent	cust_id;
	char	symbol[cSYMBOL_len+1];
	INT32	trade_qty;
	char	type_id[cTT_ID_len+1];	
} *PTradeResultFrame4Input;

typedef struct TTradeResultFrame4Output
{	
	double	comm_rate;
	char	s_name[cS_NAME_len+1];
	INT32	status;
} *PTradeResultFrame4Output;

typedef struct TTradeResultFrame5Input
{
	TIdent				broker_id;
	double				comm_amount;
	char				st_completed_id[cST_ID_len+1];
	TIMESTAMP_STRUCT	trade_dts;
	TIdent				trade_id;
	double				trade_price;
} *PTradeResultFrame5Input;

typedef struct TTradeResultFrame5Output
{	
	INT32		status;
} *PTradeResultFrame5Output;

typedef struct TTradeResultFrame6Input
{
	TIdent				acct_id;
	TIMESTAMP_STRUCT	due_date;
	char				s_name[cS_NAME_len+1];
	double				se_amount;
	TIMESTAMP_STRUCT	trade_dts;
	TIdent				trade_id;
	INT32				trade_is_cash;
	INT32				trade_qty;
	char				type_name[cTT_NAME_len+1];
} *PTradeResultFrame6Input;

typedef struct TTradeResultFrame6Output
{
	double		acct_bal;
	INT32		status;
} *PTradeResultFrame6Output;


/*
*	Trade-Status
*/
typedef struct TTradeStatusTxnInput
{
	TIdent				acct_id;
}	*PTradeStatusTxnInput,
	 TTradeStatusFrame1Input,	// Single-Frame transaction
	*PTradeStatusFrame1Input;	// Single-Frame transaction

typedef struct TTradeStatusTxnOutput
{
	INT32				status;
	char				status_name[cST_NAME_len+1];
	TIdent				trade_id;
}	*PTradeStatusTxnOutput;

typedef struct TTradeStatusFrame1Output
{
	char				broker_name[cB_NAME_len+1];
	double				charge;
	char				cust_f_name[cF_NAME_len+1];
	char				cust_l_name[cL_NAME_len+1];
	char				ex_name[cEX_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	char				s_name[cS_NAME_len+1];
	INT32				status;
	char				status_name[cST_NAME_len+1];
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT	trade_dts;
	TIdent				trade_id;
	INT32				trade_qty;
	char				type_name[cTT_NAME_len+1];
}	*PTradeStatusFrame1Output;


/*
*	Trade-Update
*/
typedef struct TTradeUpdateTxnInput
{
	TIdent				acct_id;
	INT32				frame_to_execute;					// which of the frames to execute
	INT32				max_trades;
	INT32				max_updates;
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT		trade_dts;
	TIdent				trade_id[TradeUpdateFrame1MaxRows];
	TIdent				max_acct_id;
} *PTradeUpdateTxnInput;
typedef struct TTradeUpdateTxnOutput
{
	INT32				frame_executed;						// confirmation of which frame was executed
	bool				is_cash[TradeUpdateMaxRows];
	bool				is_market[TradeUpdateMaxRows];
	INT32				num_found;
	INT32				num_updated;
	INT32				status;
	TIdent				trade_list[TradeUpdateMaxRows];
} *PTradeUpdateTxnOutput;

typedef struct TTradeUpdateFrame1Input
{
	INT32				max_trades;
	INT32				max_updates;
	TIdent				trade_id[TradeUpdateFrame1MaxRows];
} *PTradeUpdateFrame1Input;

typedef struct TTradeUpdateFrame1TradeInfo
{
	double				bid_price;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;
	bool				is_market;	
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];
	double				trade_price;

	TIMESTAMP_STRUCT	trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];	

	DB_INDICATOR		trade_id_ind;	
	DB_INDICATOR		bid_price_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		is_market_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;	
	DB_INDICATOR		trade_price_ind;

	DB_INDICATOR		trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
} *PTradeUpdateFrame1TradeInfo;

typedef struct TTradeUpdateFrame1Output
{
	TTradeUpdateFrame1TradeInfo	trade_info[TradeUpdateFrame1MaxRows];
	INT32						num_found;
	INT32						num_updated;
	INT32						status;
} *PTradeUpdateFrame1Output;

typedef struct TTradeUpdateFrame2Input
{
	TIdent				acct_id;
	INT32				max_trades;
	INT32				max_updates;
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeUpdateFrame2Input;

typedef struct TTradeUpdateFrame2TradeInfo
{
	double				bid_price;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;	
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];	
	TIdent				trade_id;
	double				trade_price;
	TIMESTAMP_STRUCT	trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];

	DB_INDICATOR		bid_price_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;
	DB_INDICATOR		trade_id_ind;
	DB_INDICATOR		trade_price_ind;

	DB_INDICATOR		trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
} *PTradeUpdateFrame2TradeInfo;

typedef struct TTradeUpdateFrame2Output
{	
	TTradeUpdateFrame2TradeInfo	trade_info[TradeUpdateFrame2MaxRows];
	INT32						num_found;
	INT32						num_updated;
	INT32						status;	
} *PTradeUpdateFrame2Output;

typedef struct TTradeUpdateFrame3Input
{
	INT32				max_trades;
	INT32				max_updates;
	TIdent				max_acct_id;
	char				symbol[cSYMBOL_len+1];
	TIMESTAMP_STRUCT	trade_dts;
} *PTradeUpdateFrame3Input;

typedef struct TTradeUpdateFrame3TradeInfo
{
	TIdent				acct_id;
	double				cash_transaction_amount;
	TIMESTAMP_STRUCT	cash_transaction_dts;
	char				cash_transaction_name[cCT_NAME_len+1];
	char				exec_name[cEXEC_NAME_len+1];
	bool				is_cash;
	double				price;
	INT32				quantity;
	char				s_name[cS_NAME_len+1];
	double				settlement_amount;
	TIMESTAMP_STRUCT	settlement_cash_due_date;
	char				settlement_cash_type[cSE_CASH_TYPE_len+1];	
	TIMESTAMP_STRUCT	trade_dts;	
	TIdent				trade_id;
	char				trade_type[cTT_ID_len+1];
	char				type_name[cTT_NAME_len+1];
	TIMESTAMP_STRUCT	trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
	char				trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];

	DB_INDICATOR		acct_id_ind;
	DB_INDICATOR		cash_transaction_amount_ind;
	DB_INDICATOR		cash_transaction_dts_ind;
	DB_INDICATOR		cash_transaction_name_ind;
	DB_INDICATOR		exec_name_ind;
	DB_INDICATOR		is_cash_ind;
	DB_INDICATOR		price_ind;
	DB_INDICATOR		quantity_ind;
	DB_INDICATOR		s_name_ind;
	DB_INDICATOR		settlement_amount_ind;
	DB_INDICATOR		settlement_cash_due_date_ind;
	DB_INDICATOR		settlement_cash_type_ind;
	DB_INDICATOR		trade_dts_ind;	
	DB_INDICATOR		trade_id_ind;
	DB_INDICATOR		trade_type_ind;
	DB_INDICATOR		type_name_ind;

	DB_INDICATOR		trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
	DB_INDICATOR		trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
} *PTradeUpdateFrame3TradeInfo;

typedef struct TTradeUpdateFrame3Output
{	
	TTradeUpdateFrame3TradeInfo trade_info[TradeUpdateFrame3MaxRows];
	INT32						num_found;
	INT32						num_updated;
	INT32						status;
} *PTradeUpdateFrame3Output;

}	// namespace TPCE

#endif	// #ifndef TXN_HARNESS_STRUCTS_H
