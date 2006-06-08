/******************************************************************************
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy 
*
*	Contributors:		Doug Johnson
*
*	Description:		This file is used to "expose" the configurable driver
*						parameters. These values may be set by test sponsors
*						for testing and prototyping. The default values 
*						represent those that must be used for a compliant run.
*
******************************************************************************/
/*
*/

#ifndef DRIVER_PARAM_SETTINGS_H
#define DRIVER_PARAM_SETTINGS_H

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

// Indidual transactions setting
typedef struct TBrokerVolumeSettings
{
	INT32	tier1;	//percentage
	INT32	tier2;	//percentage
	INT32	tier3;	//percentage

	//Initialized to default settings
	TBrokerVolumeSettings() { tier1=20; tier2=60; tier3=20; }	
	bool IsValid() { return ( tier1>=0 && tier2>=0 && tier3>=0 &&
							 (tier1+tier2+tier3 == 100)); }
} *PBrokerVolumeSettings;

typedef struct TCustomerPositionSettings
{
	INT32	by_cust_id; //percentage
	INT32	by_tax_id;	 //percentage
	INT32	get_history;//percentage	
	INT32	SValue;	// for non-uniform customer selection
	INT64	AValue;	// for non-uniform customer selection; should be set to 2^x - 1

	//Initialized to default settings
	TCustomerPositionSettings() { by_cust_id=50; by_tax_id=50; get_history=50; 
								  AValue=0; SValue=0;}
	bool IsValid() { return ( by_cust_id>=0 && by_tax_id>=0 && get_history>=0 && get_history<=100 &&
							 (by_cust_id+by_tax_id == 100)); }
} *PCustomerPositionSettings;

typedef struct TMarketWatchSettings
{
	INT32	by_acct_id;	//percentage
	INT32	by_industry;	//percentage
	INT32	by_watch_list;	//percentage

	//Initialized to default settings
	TMarketWatchSettings() { by_acct_id=35; by_industry=5; by_watch_list=60; }
	bool IsValid() { return ( by_acct_id>=0 && by_industry>=0 && by_watch_list>=0 &&
							 (by_acct_id+by_industry+by_watch_list == 100)); }
} *PMarketWatchSettings;

typedef struct TSecurityDetailSettings
{
	INT32	LOBAccessPercentage;

	//Initialized to default settings
	TSecurityDetailSettings() { LOBAccessPercentage=1; }
	bool IsValid() { return ( 0 <= LOBAccessPercentage && LOBAccessPercentage <= 100 ); }
} *PSecurityDetailSettings;

typedef struct TTradeLookupSettings
{
	INT32	do_frame1;	//percentage
	INT32	do_frame2;	//percentage
	INT32	do_frame3;	//percentage
	INT32	do_frame4;	//percentage

	INT64	AValueForTradeIDGenFrame1;		// NURnd A value
	INT32	SValueForTradeIDGenFrame1;		// NURnd s value
	INT32	MaxRowsFrame1;					// Max number of trades for frame

	INT64	AValueForAccountIdFrame2;		// NURnd A value
	INT64	AValueForTimeGenFrame2;			// NURnd A value
	INT32	SValueForAccountIdFrame2;		// NURnd s value	
	INT32	SValueForTimeGenFrame2;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	INT32	MaxRowsFrame2;					// Max number of trades for frame

	INT64	AValueForAccountIdFrame3;		// NURnd A value
	INT64	AValueForTimeGenFrame3;			// NURnd A value
	INT32	SValueForAccountIdFrame3;		// NURnd s value	
	INT32	SValueForTimeGenFrame3;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	INT32	MaxRowsFrame3;					// Max number of rows for frame

	INT64	AValueForSymbolFrame4;			// NURnd A value
	INT64	AValueForTimeGenFrame4;			// NURnd A value
	INT32	SValueForSymbolFrame4;			// NURnd s value	
	INT32	SValueForTimeGenFrame4;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame4;	// Used to cap time interval generated.
	INT32	MaxRowsFrame4;					// Max number of trades for frame

	//Initialized to default settings
	TTradeLookupSettings() { do_frame1=30; do_frame2=30; do_frame3=10; do_frame4=30;
							AValueForTradeIDGenFrame1=1023; SValueForTradeIDGenFrame1=7; 
							MaxRowsFrame1=10;
							AValueForAccountIdFrame2=0; SValueForAccountIdFrame2=0;
							AValueForTimeGenFrame2=127; SValueForTimeGenFrame2=3;
							NumSFOffsetsFromEndTimeFrame2=3;
							MaxRowsFrame2=10;
							AValueForAccountIdFrame3=0; SValueForAccountIdFrame3=0;
							AValueForTimeGenFrame3=0; SValueForTimeGenFrame3=0;
							NumSFOffsetsFromEndTimeFrame3=3;
							MaxRowsFrame3=20;
							AValueForSymbolFrame4=0; SValueForSymbolFrame4=0;
							AValueForTimeGenFrame4=0; SValueForTimeGenFrame4=0;
							NumSFOffsetsFromEndTimeFrame4=3;
							MaxRowsFrame4=10;
							}
	bool IsValid() { return ( do_frame1>=0 && do_frame2>=0 && do_frame3>=0 && do_frame4>=0 &&
							 (do_frame1+do_frame2+do_frame3+do_frame4 == 100) &&
							 MaxRowsFrame1 <= TradeLookupFrame1MaxRows &&
							 MaxRowsFrame2 <= TradeLookupFrame2MaxRows &&
							 MaxRowsFrame3 <= TradeLookupFrame3MaxRows &&
							 MaxRowsFrame4 <= TradeLookupFrame4MaxRows );
							}
} *PTradeLookupSettings;

typedef struct TTradeOrderSettings
{
	INT32	market;
	INT32	limit;
	INT32	stop_loss;
	INT32	security_by_name;
	INT32	security_by_symbol;
	INT32	buy_orders;
	INT32	sell_orders;
	INT32	lifo;
	INT32	exec_is_owner;
	INT32	rollback;
	INT32	type_is_margin;

	//Initialized to default settings
	TTradeOrderSettings() { market=60; limit=40; stop_loss=50;
							security_by_name=40; security_by_symbol=60;
							buy_orders=50; sell_orders=50;
							lifo=35; exec_is_owner=90; rollback=1; 
							type_is_margin=16;}
	bool IsValid() { return( market>=0 && limit>=0 && stop_loss>=0 && stop_loss<=100 &&
							 security_by_name>=0 && security_by_symbol>=0 &&
							 buy_orders>=0 && sell_orders>=0 && lifo>=0 && lifo<=100 &&
							 exec_is_owner>=0 && exec_is_owner<=100 &&
							 rollback>=0 && rollback<=100 &&
							 type_is_margin>=0 && type_is_margin<=100 &&
							 (market+limit == 100) &&							 
							 (security_by_name+security_by_symbol == 100) &&
							 (buy_orders+sell_orders == 100) 
						   ); }
} *PTradeOrderSettings;

typedef struct TTradeUpdateSettings
{
	INT32	do_frame1;	//percentage
	INT32	do_frame2;	//percentage	

	INT64	AValueForTradeIDGenFrame1;		// NURnd A value
	INT32	SValueForTradeIDGenFrame1;		// NURnd s value
	INT32	MaxRowsFrame1;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame1;			// Max number of rows to update

	INT32	do_frame3;	//percentage	- not actually used (just for padding)

	INT64	AValueForAccountIdFrame2;		// NURnd A value
	INT64	AValueForTimeGenFrame2;			// NURnd A value
	INT32	SValueForAccountIdFrame2;		// NURnd s value
	INT32	SValueForTimeGenFrame2;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	INT32	MaxRowsFrame2;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame2;			// Max number of rows to update
	
	INT32	SValueForSymbolFrame3;			// NURnd s value
	INT64	AValueForSymbolFrame3;			// NURnd A value
	INT64	AValueForTimeGenFrame3;			// NURnd A value
	INT32	SValueForTimeGenFrame3;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	INT32	MaxRowsFrame3;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame3;			// Max number of rows to update

	//Initialized to default settings
	TTradeUpdateSettings() { do_frame1=33; do_frame2=33; do_frame3=34;
							MaxRowsFrame1=10;
							MaxRowsToUpdateFrame1=MaxRowsFrame1;
							AValueForTradeIDGenFrame1=1023; SValueForTradeIDGenFrame1=7; 
							MaxRowsFrame2=10;
							MaxRowsToUpdateFrame2=MaxRowsFrame2;
							AValueForAccountIdFrame2=0; SValueForAccountIdFrame2=0;
							AValueForTimeGenFrame2=127; SValueForTimeGenFrame2=3;
							NumSFOffsetsFromEndTimeFrame2=3;
							MaxRowsFrame3=10;
							MaxRowsToUpdateFrame3=MaxRowsFrame3;
							AValueForSymbolFrame3=0; SValueForSymbolFrame3=0;
							AValueForTimeGenFrame3=0; SValueForTimeGenFrame3=0;
							NumSFOffsetsFromEndTimeFrame3=3;
							}
	bool IsValid() { return ( do_frame1>=0 && do_frame2>=0 && do_frame3>=0 &&
							 (do_frame1+do_frame2+do_frame3 == 100) &&
							 MaxRowsFrame1 <= TradeUpdateFrame1MaxRows &&
							 MaxRowsFrame2 <= TradeUpdateFrame2MaxRows &&
							 MaxRowsFrame3 <= TradeUpdateFrame3MaxRows &&
							 MaxRowsToUpdateFrame1 <= TradeUpdateFrame1MaxRows &&
							 MaxRowsToUpdateFrame2 <= TradeUpdateFrame2MaxRows &&
							 MaxRowsToUpdateFrame3 <= TradeUpdateFrame3MaxRows);
							}
} *PTradeUpdateSettings;

typedef struct TTxnMixGeneratorSettings
{
	// Market-Feed and Trade-Result settings don't really alter the mix.
	// They are done as a by-product of Trade-Orders. However, the values
	// still need to be set correctly because they get used when generating
	// the random number for selecting the other transaction types.
	//
	// Transaction mix levels are expressed out of a total of 1000.
	INT32	BrokerVolumeMixLevel;
	INT32	CustomerPositionMixLevel;
	INT32	MarketFeedMixLevel;
	INT32	MarketWatchMixLevel;
	INT32	SecurityDetailMixLevel;
	INT32	TradeLookupMixLevel;
	INT32	TradeOrderMixLevel;
	INT32	TradeResultMixLevel;
	INT32	TradeStatusMixLevel;
	INT32	TradeUpdateMixLevel;

	INT32	TransactionMixTotal;

	//Initialized to default settings
	TTxnMixGeneratorSettings()
	{
		BrokerVolumeMixLevel		=    9;
		CustomerPositionMixLevel	=  140;
		MarketFeedMixLevel			=   10;
		MarketWatchMixLevel			=  190;
		SecurityDetailMixLevel		=  150;
		TradeLookupMixLevel			=   80;
		TradeOrderMixLevel			=  101;
		TradeResultMixLevel			=  100;
		TradeStatusMixLevel			=  200;
		TradeUpdateMixLevel			=   20;		
	}

	bool IsValid()
	{
		return( BrokerVolumeMixLevel >= 0		&&
				CustomerPositionMixLevel >= 0	&&
				MarketFeedMixLevel >= 0			&&
				MarketWatchMixLevel >= 0		&&
				SecurityDetailMixLevel >= 0		&&
				TradeLookupMixLevel >= 0		&&
				TradeOrderMixLevel >= 0			&&
				TradeResultMixLevel >= 0		&&
				TradeStatusMixLevel >= 0		&&
				TradeUpdateMixLevel >= 0
				);
	}
} *PTxnMixGeneratorSettings;

// Parameter skew settings for all the transactions
typedef struct TParameterSettings
{
	TBrokerVolumeSettings		BV_settings;
	TCustomerPositionSettings	CP_settings;
	TMarketWatchSettings		MW_settings;
	TSecurityDetailSettings		SD_settings;
	TTradeLookupSettings		TL_settings;
	TTradeOrderSettings			TO_settings;
	TTradeUpdateSettings		TU_settings;

	TTxnMixGeneratorSettings	TxnMixGenerator_settings;

	bool IsValid()
	{
		return( BV_settings.IsValid() && CP_settings.IsValid() && 
				MW_settings.IsValid() && SD_settings.IsValid() &&
				TL_settings.IsValid() && TO_settings.IsValid() &&
				TU_settings.IsValid() &&
				TxnMixGenerator_settings.IsValid()
				);
	}
} *PParameterSettings;

}	// namespace TPCE

#endif	//#ifndef DRIVER_PARAM_SETTINGS_H
