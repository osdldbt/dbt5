/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Doug Johnson, Matt Emmerton
 */

/******************************************************************************
*	Description:		This file is used to "expose" the configurable driver
*						parameters. These values may be set by test sponsors
*						for testing and prototyping. The default values 
*						represent those that must be used for a compliant run.
******************************************************************************/

#ifndef DRIVER_PARAM_SETTINGS_H
#define DRIVER_PARAM_SETTINGS_H

#include <iostream>
#include <iomanip>				// for log message formatting
#include <sstream>				// for log message construction

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

/******************************************************************************
*	Parameter Base Class Template
******************************************************************************/
template < typename T > class CParametersWithoutDefaults
{
public:
	T	cur;

	CParametersWithoutDefaults() {}

	virtual bool IsValid(void) = 0;
};

template < typename T, typename T2 > class CParametersWithDefaults
{
//protected:
public:
	T	dft;
	T2	state;

public:
	T	cur;

	CParametersWithDefaults() {}

	void Initialize(void)
	{
		InitializeDefaults();
		SetToDefaults();
	}

	void SetToDefaults(void)
	{
		cur = dft;
		CheckDefaults();
	}

	virtual void InitializeDefaults(void) {}
	virtual void CheckDefaults(void) {}
	virtual bool IsValid(void) = 0;
};

/******************************************************************************
*	Parameter Structures (Data)
******************************************************************************/
typedef struct TBrokerVolumeSettings
{
} *PBrokerVolumeSettings;

typedef struct TCustomerPositionSettings
{
	INT32	by_cust_id;		// percentage
	INT32	by_tax_id;		// percentage
	INT32	get_history;	// percentage	
	INT32	AValue;			// for non-uniform customer selection
	INT32	SValue;			// for non-uniform customer selection
} *PCustomerPositionSettings;

typedef struct TMarketWatchSettings
{
	INT32	by_acct_id;		// percentage
	INT32	by_industry;	// percentage
	INT32	by_watch_list;	// percentage
} *PMarketWatchSettings;

typedef struct TSecurityDetailSettings
{
	INT32	LOBAccessPercentage;
} *PSecurityDetailSettings;

typedef struct TTradeLookupSettings
{
	INT32	do_frame1;						// percentage
	INT32	do_frame2;						// percentage
	INT32	do_frame3;						// percentage
	INT32	do_frame4;						// percentage

	INT32	AValueForTradeIDGenFrame1;		// NURnd A value
	INT32	SValueForTradeIDGenFrame1;		// NURnd s value
	INT32	MaxRowsFrame1;					// Max number of trades for frame

	INT32	AValueForTimeGenFrame2;			// NURnd A value
	INT32	SValueForTimeGenFrame2;			// NURnd s value
	INT32	AValueForAccountIdFrame2;		// NURnd A value
	INT32	SValueForAccountIdFrame2;		// NURnd S value
	INT32	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	INT32	MaxRowsFrame2;					// Max number of trades for frame

	INT32	AValueForSymbolFrame3;			// NURnd A value
	INT32	SValueForSymbolFrame3;			// NURnd s value	
	INT32	AValueForTimeGenFrame3;			// NURnd A value
	INT32	SValueForTimeGenFrame3;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	INT32	MaxRowsFrame3;					// Max number of trades for frame

	INT32	AValueForTimeGenFrame4;			// NURnd A value
	INT32	SValueForTimeGenFrame4;			// NURnd s value
	INT32	AValueForAccountIdFrame4;		// NURnd A value
	INT32	SValueForAccountIdFrame4;		// NURnd S value
	INT32	NumSFOffsetsFromEndTimeFrame4;	// Used to cap time interval generated.
	INT32	MaxRowsFrame4;					// Max number of rows for frame
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
} *PTradeOrderSettings;

typedef struct TTradeUpdateSettings
{
	INT32	do_frame1;	//percentage
	INT32	do_frame2;	//percentage	
	INT32	do_frame3;	//percentage	

	INT32	AValueForTradeIDGenFrame1;		// NURnd A value
	INT32	SValueForTradeIDGenFrame1;		// NURnd s value
	INT32	MaxRowsFrame1;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame1;			// Max number of rows to update

	INT32	AValueForTimeGenFrame2;			// NURnd A value
	INT32	SValueForTimeGenFrame2;			// NURnd s value
	INT32	AValueForAccountIdFrame2;		// NURnd A value
	INT32	SValueForAccountIdFrame2;		// NURnd S value
	INT32	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	INT32	MaxRowsFrame2;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame2;			// Max number of rows to update
	
	INT32	AValueForSymbolFrame3;			// NURnd A value
	INT32	SValueForSymbolFrame3;			// NURnd s value
	INT32	AValueForTimeGenFrame3;			// NURnd A value
	INT32	SValueForTimeGenFrame3;			// NURnd s value
	INT32	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	INT32	MaxRowsFrame3;					// Max number of trades for frame
	INT32	MaxRowsToUpdateFrame3;			// Max number of rows to update
} *PTradeUpdateSettings;

typedef struct TTxnMixGeneratorSettings
{
	// Market-Feed and Trade-Result settings don't really alter the mix.
	// They are done as a by-product of Trade-Orders. However, the values
	// still need to be set correctly because they get used when generating
	// the random number for selecting the other transaction types.
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

	// Transaction mix levels are expressed out of a total of 1000.
	INT32	TransactionMixTotal;
} *PTxnMixGeneratorSettings;

typedef struct TLoaderSettings
{
	TIdent	iConfiguredCustomerCount;
	TIdent	iActiveCustomerCount;
	INT32	iScaleFactor;
	INT32	iDaysOfInitialTrades;
	TIdent	iStartingCustomer;
	TIdent	iCustomerCount;
} *pLoaderSettings;

typedef struct TDriverGlobalSettings
{
	TIdent	iConfiguredCustomerCount;
	TIdent	iActiveCustomerCount;
	INT32	iScaleFactor;
	INT32	iDaysOfInitialTrades;
} *PDriverGlobalSettings;

typedef struct TDriverCESettings
{
	UINT32	UniqueId;
	RNGSEED	TxnMixRNGSeed;
	RNGSEED	TxnInputRNGSeed;
} *PDriverCESettings;

typedef struct TDriverCEPartitionSettings
{
	TIdent	iMyStartingCustomerId;
	TIdent	iMyCustomerCount;
	INT32	iPartitionPercent;
} *PDriverCEPartitionSettings;

typedef struct TDriverMEESettings
{
	UINT32	UniqueId;
	RNGSEED	RNGSeed;
	RNGSEED	TickerTapeRNGSeed;
	RNGSEED	TradingFloorRNGSeed;
} *PDriverMEESettings;

typedef struct TDriverDMSettings
{
	UINT32	UniqueId;
	RNGSEED	RNGSeed;
} *PDriverDMSettings;

/******************************************************************************
*	Parameter Structures (Boolean "Is Default" State)
******************************************************************************/
struct TBrokerVolumeSettingsState
{
};

struct TCustomerPositionSettingsState
{
	bool	by_cust_id;		// percentage
	bool	by_tax_id;		// percentage
	bool	get_history;	// percentage	
	bool	AValue;			// for non-uniform customer selection
	bool	SValue;			// for non-uniform customer selection
};

struct TMarketWatchSettingsState
{
	bool	by_acct_id;		// percentage
	bool	by_industry;	// percentage
	bool	by_watch_list;	// percentage
};

struct TSecurityDetailSettingsState
{
	bool	LOBAccessPercentage;
};

struct TTradeLookupSettingsState
{
	bool	do_frame1;						// percentage
	bool	do_frame2;						// percentage
	bool	do_frame3;						// percentage
	bool	do_frame4;						// percentage

	bool	AValueForTradeIDGenFrame1;		// NURnd A value
	bool	SValueForTradeIDGenFrame1;		// NURnd s value
	bool	MaxRowsFrame1;					// Max number of trades for frame

	bool	AValueForTimeGenFrame2;			// NURnd A value
	bool	SValueForTimeGenFrame2;			// NURnd s value
	bool	AValueForAccountIdFrame2;		// NURnd A value
	bool	SValueForAccountIdFrame2;		// NURnd s value
	bool	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	bool	MaxRowsFrame2;					// Max number of trades for frame

	bool	AValueForSymbolFrame3;			// NURnd A value
	bool	SValueForSymbolFrame3;			// NURnd s value	
	bool	AValueForTimeGenFrame3;			// NURnd A value
	bool	SValueForTimeGenFrame3;			// NURnd s value
	bool	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	bool	MaxRowsFrame3;					// Max number of trades for frame

	bool	AValueForTimeGenFrame4;			// NURnd A value
	bool	SValueForTimeGenFrame4;			// NURnd s value
	bool	AValueForAccountIdFrame4;		// NURnd A value
	bool	SValueForAccountIdFrame4;		// NURnd s value
	bool	NumSFOffsetsFromEndTimeFrame4;	// Used to cap time interval generated.
	bool	MaxRowsFrame4;					// Max number of rows for frame
};

struct TTradeOrderSettingsState
{
	bool	market;
	bool	limit;
	bool	stop_loss;
	bool	security_by_name;
	bool	security_by_symbol;
	bool	buy_orders;
	bool	sell_orders;
	bool	lifo;
	bool	exec_is_owner;
	bool	rollback;
	bool	type_is_margin;
};

struct TTradeUpdateSettingsState
{
	bool	do_frame1;	//percentage
	bool	do_frame2;	//percentage	
	bool	do_frame3;	//percentage	

	bool	AValueForTradeIDGenFrame1;		// NURnd A value
	bool	SValueForTradeIDGenFrame1;		// NURnd s value
	bool	MaxRowsFrame1;					// Max number of trades for frame
	bool	MaxRowsToUpdateFrame1;			// Max number of rows to update

	bool	AValueForTimeGenFrame2;			// NURnd A value
	bool	SValueForTimeGenFrame2;			// NURnd s value
	bool	AValueForAccountIdFrame2;		// NURnd A value
	bool	SValueForAccountIdFrame2;		// NURnd s value
	bool	NumSFOffsetsFromEndTimeFrame2;	// Used to cap time interval generated.
	bool	MaxRowsFrame2;					// Max number of trades for frame
	bool	MaxRowsToUpdateFrame2;			// Max number of rows to update
	
	bool	AValueForSymbolFrame3;			// NURnd A value
	bool	SValueForSymbolFrame3;			// NURnd s value
	bool	AValueForTimeGenFrame3;			// NURnd A value
	bool	SValueForTimeGenFrame3;			// NURnd s value
	bool	NumSFOffsetsFromEndTimeFrame3;	// Used to cap time interval generated.
	bool	MaxRowsFrame3;					// Max number of trades for frame
	bool	MaxRowsToUpdateFrame3;			// Max number of rows to update
};

struct TTxnMixGeneratorSettingsState
{
	bool	BrokerVolumeMixLevel;
	bool	CustomerPositionMixLevel;
	bool	MarketFeedMixLevel;
	bool	MarketWatchMixLevel;
	bool	SecurityDetailMixLevel;
	bool	TradeLookupMixLevel;
	bool	TradeOrderMixLevel;
	bool	TradeResultMixLevel;
	bool	TradeStatusMixLevel;
	bool	TradeUpdateMixLevel;
	bool	TransactionMixTotal;
};

struct TLoaderSettingsState
{
	bool	iConfiguredCustomerCount;
	bool	iActiveCustomerCount;
	bool	iScaleFactor;
	bool	iDaysOfInitialTrades;
	bool	iStartingCustomer;
	bool	iCustomerCount;
};

struct TDriverGlobalSettingsState
{
	bool	iConfiguredCustomerCount;
	bool	iActiveCustomerCount;
	bool	iScaleFactor;
	bool	iDaysOfInitialTrades;
};

/******************************************************************************
*	Parameter Derived Class / Template Instantiation
******************************************************************************/
class CBrokerVolumeSettings : public CParametersWithDefaults< struct TBrokerVolumeSettings, struct TBrokerVolumeSettingsState >
{
public:

	CBrokerVolumeSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
	}

	void CheckDefaults(void)
	{
	}

	bool IsValid(void)
	{
		return ( true ) ;
	}
};

class CCustomerPositionSettings : public CParametersWithDefaults< struct TCustomerPositionSettings, struct TCustomerPositionSettingsState >
{
public:
	CCustomerPositionSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.by_cust_id = 50;
		dft.by_tax_id = 50;
		dft.get_history = 50;
		dft.AValue = 0;
		dft.SValue = 0;
	}

	void CheckDefaults(void)
	{
		state.by_cust_id = (cur.by_cust_id == dft.by_cust_id);
		state.by_tax_id = (cur.by_tax_id == dft.by_tax_id);
		state.get_history = (cur.get_history == dft.get_history);
		state.AValue = (cur.AValue == dft.AValue);
		state.SValue = (cur.SValue == dft.SValue);
	}

	bool IsValid(void)
	{
		return ( cur.by_cust_id >= 0 && cur.by_cust_id <= 100 &&
				 cur.by_tax_id >= 0 && cur.by_tax_id <= 100 &&
				 cur.get_history >= 0 && cur.get_history <= 100 &&
				 (cur.by_cust_id + cur.by_tax_id == 100) );
	}
};

class CMarketWatchSettings : public CParametersWithDefaults< struct TMarketWatchSettings, struct TMarketWatchSettingsState >
{
public:
	CMarketWatchSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.by_acct_id = 35;
		dft.by_industry = 5;
		dft.by_watch_list = 60;
	}

	void CheckDefaults(void)
	{
		state.by_acct_id = (cur.by_acct_id == dft.by_acct_id);
		state.by_industry = (cur.by_industry == dft.by_industry);
		state.by_watch_list = (cur.by_watch_list == dft.by_watch_list);
	}

	bool IsValid(void)
	{
		return ( cur.by_acct_id >= 0 && cur.by_acct_id <= 100 &&
				 cur.by_industry >= 0 && cur.by_industry <= 100 &&
				 cur.by_watch_list >=0 && cur.by_watch_list <= 100 &&
				 (cur.by_acct_id + cur.by_industry + cur.by_watch_list == 100) );
	}
};

class CSecurityDetailSettings : public CParametersWithDefaults< struct TSecurityDetailSettings, struct TSecurityDetailSettingsState >
{
public:
	CSecurityDetailSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.LOBAccessPercentage = 1;
	}

	void CheckDefaults(void)
	{
		state.LOBAccessPercentage = (cur.LOBAccessPercentage == dft.LOBAccessPercentage);
	}

	bool IsValid(void)
	{
		return ( cur.LOBAccessPercentage >= 0 && cur.LOBAccessPercentage <= 100 ) ;
	}
};

class CTradeLookupSettings : public CParametersWithDefaults< struct TTradeLookupSettings, struct TTradeLookupSettingsState >
{
public:
	CTradeLookupSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.do_frame1 = 30;
		dft.do_frame2 = 30;
		dft.do_frame3 = 30;
		dft.do_frame4 = 10;
		dft.AValueForTradeIDGenFrame1 = 1023;
		dft.SValueForTradeIDGenFrame1 = 7; 
		dft.MaxRowsFrame1 = 20;
		dft.AValueForTimeGenFrame2 = 127;
		dft.SValueForTimeGenFrame2 = 3;
		dft.AValueForAccountIdFrame2 = 0;
		dft.SValueForAccountIdFrame2 = 0;
		dft.NumSFOffsetsFromEndTimeFrame2 = 3;
		dft.MaxRowsFrame2 = 20;
		dft.AValueForSymbolFrame3 = 0;
		dft.SValueForSymbolFrame3 = 0;
		dft.AValueForTimeGenFrame3 = 0;
		dft.SValueForTimeGenFrame3 = 0;
		dft.NumSFOffsetsFromEndTimeFrame3 = 3;
		dft.MaxRowsFrame3 = 20;
		dft.AValueForTimeGenFrame4 = 0;
		dft.SValueForTimeGenFrame4 = 0;
		dft.AValueForAccountIdFrame4 = 0;
		dft.SValueForAccountIdFrame4 = 0;
		dft.NumSFOffsetsFromEndTimeFrame4 = 3;
		dft.MaxRowsFrame4 = 20;
	}

	void CheckDefaults(void)
	{
		state.do_frame1 = (cur.do_frame1 == dft.do_frame1);
		state.do_frame2 = (cur.do_frame2 == dft.do_frame2);
		state.do_frame3 = (cur.do_frame3 == dft.do_frame3);
		state.do_frame4 = (cur.do_frame4 == dft.do_frame4);
		state.AValueForTradeIDGenFrame1 = (cur.AValueForTradeIDGenFrame1 == dft.AValueForTradeIDGenFrame1);
		state.SValueForTradeIDGenFrame1 = (cur.SValueForTradeIDGenFrame1 == dft.SValueForTradeIDGenFrame1);
		state.MaxRowsFrame1 = (cur.MaxRowsFrame1 == dft.MaxRowsFrame1);
		state.AValueForTimeGenFrame2 = (cur.AValueForTimeGenFrame2 == dft.AValueForTimeGenFrame2);
		state.SValueForTimeGenFrame2 = (cur.SValueForTimeGenFrame2 == dft.SValueForTimeGenFrame2);
		state.AValueForAccountIdFrame2 = (cur.AValueForAccountIdFrame2 == dft.AValueForAccountIdFrame2);
		state.SValueForAccountIdFrame2 = (cur.SValueForAccountIdFrame2 == dft.SValueForAccountIdFrame2);
		state.NumSFOffsetsFromEndTimeFrame2 = (cur.NumSFOffsetsFromEndTimeFrame2 == dft.NumSFOffsetsFromEndTimeFrame2);
		state.MaxRowsFrame2 = (cur.MaxRowsFrame2 == dft.MaxRowsFrame2);
		state.AValueForSymbolFrame3 = (cur.AValueForSymbolFrame3 == dft.AValueForSymbolFrame3);
		state.SValueForSymbolFrame3 = (cur.SValueForSymbolFrame3 == dft.SValueForSymbolFrame3);
		state.AValueForTimeGenFrame3 = (cur.AValueForTimeGenFrame3 == dft.AValueForTimeGenFrame3);
		state.SValueForTimeGenFrame3 = (cur.SValueForTimeGenFrame3 == dft.SValueForTimeGenFrame3);
		state.NumSFOffsetsFromEndTimeFrame3 = (cur.NumSFOffsetsFromEndTimeFrame3 == dft.NumSFOffsetsFromEndTimeFrame3);
		state.MaxRowsFrame3 = (cur.MaxRowsFrame3 == dft.MaxRowsFrame3);
		state.AValueForTimeGenFrame4 = (cur.AValueForTimeGenFrame4 == dft.AValueForTimeGenFrame4);
		state.SValueForTimeGenFrame4 = (cur.SValueForTimeGenFrame4 == dft.SValueForTimeGenFrame4);
		state.AValueForAccountIdFrame4 = (cur.AValueForAccountIdFrame4 == dft.AValueForAccountIdFrame4);
		state.SValueForAccountIdFrame4 = (cur.SValueForAccountIdFrame4 == dft.SValueForAccountIdFrame4);
		state.NumSFOffsetsFromEndTimeFrame4 = (cur.NumSFOffsetsFromEndTimeFrame4 == dft.NumSFOffsetsFromEndTimeFrame4);
		state.MaxRowsFrame4 = (cur.MaxRowsFrame4 == dft.MaxRowsFrame4);
	}

	bool IsValid(void)
	{
		return ( cur.do_frame1 >= 0 && cur.do_frame1 <= 100 &&
				 cur.do_frame2 >= 0 && cur.do_frame2 <= 100 &&
				 cur.do_frame3 >= 0 && cur.do_frame3 <= 100 &&
				 cur.do_frame4 >= 0 && cur.do_frame4 <= 100 &&
				 (cur.do_frame1 + cur.do_frame2 + cur.do_frame3 + cur.do_frame4 == 100) &&
				 cur.MaxRowsFrame1 <= TradeLookupFrame1MaxRows &&
				 cur.MaxRowsFrame2 <= TradeLookupFrame2MaxRows &&
				 cur.MaxRowsFrame3 <= TradeLookupFrame3MaxRows &&
				 cur.MaxRowsFrame4 <= TradeLookupFrame4MaxRows );
	}
};

class CTradeOrderSettings : public CParametersWithDefaults< struct TTradeOrderSettings, struct TTradeOrderSettingsState >
{
public:
	CTradeOrderSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.market = 60;
		dft.limit = 40;
		dft.stop_loss = 50;
		dft.security_by_name = 40;
		dft.security_by_symbol = 60;
		dft.buy_orders = 50;
		dft.sell_orders = 50;
		dft.lifo = 35;
		dft.exec_is_owner = 90;
		dft.rollback = 1;
		dft.type_is_margin = 16;
	}

	void CheckDefaults(void)
	{
		state.market = (cur.market == dft.market);
		state.limit = (cur.limit == dft.limit);
		state.stop_loss = (cur.stop_loss == dft.stop_loss);
		state.security_by_name = (cur.security_by_name == dft.security_by_name);
		state.security_by_symbol = (cur.security_by_symbol == dft.security_by_symbol);
		state.buy_orders = (cur.buy_orders == dft.buy_orders);
		state.sell_orders = (cur.sell_orders == dft.sell_orders);
		state.lifo = (cur.lifo == dft.lifo);
		state.exec_is_owner = (cur.exec_is_owner == dft.exec_is_owner);
		state.rollback = (cur.rollback == dft.rollback);
		state.type_is_margin = (cur.type_is_margin == dft.type_is_margin);
	}

	bool IsValid(void)
	{
		return( cur.market >= 0 && cur.market <= 100 &&
				cur.limit >= 0 && cur.limit <= 100 &&
				(cur.market + cur.limit == 100) &&
				cur.stop_loss >= 0 && cur.stop_loss <= 100 &&
				cur.security_by_name >= 0 && cur.security_by_name <= 100 &&
				cur.security_by_symbol >= 0 && cur.security_by_symbol <= 100 &&
				(cur.security_by_name + cur.security_by_symbol == 100) &&
				cur.buy_orders >= 0 && cur.buy_orders <= 100 &&
				cur.sell_orders >= 0 && cur.sell_orders <= 100 &&
				(cur.buy_orders + cur.sell_orders == 100) &&
				cur.lifo >= 0 && cur.lifo <= 100 &&
				cur.exec_is_owner >= 60 && cur.exec_is_owner <= 100 &&	
				cur.rollback >= 0 && cur.rollback <= 100 &&
				cur.type_is_margin >= 0 && cur.type_is_margin <= 100); 
	}
};

class CTradeUpdateSettings : public CParametersWithDefaults< struct TTradeUpdateSettings, struct TTradeUpdateSettingsState >
{
public:
	CTradeUpdateSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.do_frame1 = 33;
		dft.do_frame2 = 33;
		dft.do_frame3 = 34;
		dft.MaxRowsFrame1 = 20;
		dft.MaxRowsToUpdateFrame1 = 5;
		dft.AValueForTradeIDGenFrame1 = 1023;
		dft.SValueForTradeIDGenFrame1 = 7; 
		dft.MaxRowsFrame2 = 20;
		dft.MaxRowsToUpdateFrame2 = 5;
		dft.AValueForTimeGenFrame2 = 127;
		dft.SValueForTimeGenFrame2 = 3;
		dft.AValueForAccountIdFrame2 = 0;
		dft.SValueForAccountIdFrame2 = 0;
		dft.NumSFOffsetsFromEndTimeFrame2 = 3;
		dft.MaxRowsFrame3 = 20;
		dft.MaxRowsToUpdateFrame3 = 5;
		dft.AValueForSymbolFrame3 = 0;
		dft.SValueForSymbolFrame3 = 0;
		dft.AValueForTimeGenFrame3 = 0;
		dft.SValueForTimeGenFrame3 = 0;
		dft.NumSFOffsetsFromEndTimeFrame3 = 3;
	}

	void CheckDefaults(void)
	{
		state.do_frame1 = (cur.do_frame1 == dft.do_frame1);
		state.do_frame2 = (cur.do_frame2 == dft.do_frame2);
		state.do_frame3 = (cur.do_frame3 == dft.do_frame3);
		state.MaxRowsFrame1 = (cur.MaxRowsFrame1 == dft.MaxRowsFrame1);
		state.MaxRowsToUpdateFrame1 = (cur.MaxRowsToUpdateFrame1 == dft.MaxRowsToUpdateFrame1);
		state.AValueForTradeIDGenFrame1 = (cur.AValueForTradeIDGenFrame1 == dft.AValueForTradeIDGenFrame1);
		state.SValueForTradeIDGenFrame1 = (cur.SValueForTradeIDGenFrame1 == dft.SValueForTradeIDGenFrame1);
		state.MaxRowsFrame2 = (cur.MaxRowsFrame2 == dft.MaxRowsFrame2);
		state.MaxRowsToUpdateFrame2 = (cur.MaxRowsToUpdateFrame2 == dft.MaxRowsToUpdateFrame2);
		state.AValueForTimeGenFrame2 = (cur.AValueForTimeGenFrame2 == dft.AValueForTimeGenFrame2);
		state.SValueForTimeGenFrame2 = (cur.SValueForTimeGenFrame2 == dft.SValueForTimeGenFrame2);
		state.AValueForAccountIdFrame2 = (cur.AValueForAccountIdFrame2 == dft.AValueForAccountIdFrame2);
		state.SValueForAccountIdFrame2 = (cur.SValueForAccountIdFrame2 == dft.SValueForAccountIdFrame2);
		state.NumSFOffsetsFromEndTimeFrame2 = (cur.NumSFOffsetsFromEndTimeFrame2 == dft.NumSFOffsetsFromEndTimeFrame2);
		state.MaxRowsFrame3 = (cur.MaxRowsFrame3 == dft.MaxRowsFrame3);
		state.MaxRowsToUpdateFrame3 = (cur.MaxRowsToUpdateFrame3 == dft.MaxRowsToUpdateFrame3);
		state.AValueForSymbolFrame3 = (cur.AValueForSymbolFrame3 == dft.AValueForSymbolFrame3);
		state.SValueForSymbolFrame3 = (cur.SValueForSymbolFrame3 == dft.SValueForSymbolFrame3);
		state.AValueForTimeGenFrame3 = (cur.AValueForTimeGenFrame3 == dft.AValueForTimeGenFrame3);
		state.SValueForTimeGenFrame3 = (cur.SValueForTimeGenFrame3 == dft.SValueForTimeGenFrame3);
		state.NumSFOffsetsFromEndTimeFrame3 = (cur.NumSFOffsetsFromEndTimeFrame3 == dft.NumSFOffsetsFromEndTimeFrame3);
	}

	bool IsValid(void)
	{
		return ( cur.do_frame1 >= 0 && cur.do_frame1 <= 100 &&
				 cur.do_frame2 >= 0 && cur.do_frame2 <= 100 &&
				 cur.do_frame3 >= 0 && cur.do_frame3 <= 100 &&
				 (cur.do_frame1 + cur.do_frame2 + cur.do_frame3 == 100) &&
				 cur.MaxRowsFrame1 <= TradeUpdateFrame1MaxRows &&
				 cur.MaxRowsFrame2 <= TradeUpdateFrame2MaxRows &&
				 cur.MaxRowsFrame3 <= TradeUpdateFrame3MaxRows &&
				 cur.MaxRowsToUpdateFrame1 <= TradeUpdateFrame1MaxRows &&
				 cur.MaxRowsToUpdateFrame2 <= TradeUpdateFrame2MaxRows &&
				 cur.MaxRowsToUpdateFrame3 <= TradeUpdateFrame3MaxRows );
	}
};

class CTxnMixGeneratorSettings : public CParametersWithDefaults< struct TTxnMixGeneratorSettings, struct TTxnMixGeneratorSettingsState >
{
public:
	CTxnMixGeneratorSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		dft.BrokerVolumeMixLevel		=   49;
		dft.CustomerPositionMixLevel	=  130;
		dft.MarketFeedMixLevel			=   10;
		dft.MarketWatchMixLevel			=  180;
		dft.SecurityDetailMixLevel		=  140;
		dft.TradeLookupMixLevel			=   80;
		dft.TradeOrderMixLevel			=  101;
		dft.TradeResultMixLevel			=  100;
		dft.TradeStatusMixLevel			=  190;
		dft.TradeUpdateMixLevel			=   20;		
	}

	bool IsValid(void)
	{
		return( cur.BrokerVolumeMixLevel >= 0		&& cur.BrokerVolumeMixLevel <= 1000      &&
				cur.CustomerPositionMixLevel >= 0	&& cur.CustomerPositionMixLevel <= 1000  &&
				cur.MarketFeedMixLevel >= 0			&& cur.MarketFeedMixLevel <= 1000        &&
				cur.MarketWatchMixLevel >= 0		&& cur.MarketWatchMixLevel <= 1000       &&
				cur.SecurityDetailMixLevel >= 0		&& cur.SecurityDetailMixLevel <= 1000    && 
				cur.TradeLookupMixLevel >= 0		&& cur.TradeLookupMixLevel <= 1000       &&
				cur.TradeOrderMixLevel >= 0			&& cur.TradeOrderMixLevel <= 1000        &&
				cur.TradeResultMixLevel >= 0		&& cur.TradeResultMixLevel <= 1000       &&
				cur.TradeStatusMixLevel >= 0		&& cur.TradeStatusMixLevel <= 1000       &&
				cur.TradeUpdateMixLevel >= 0        && cur.TradeUpdateMixLevel <= 1000       &&
				(cur.BrokerVolumeMixLevel   + cur.CustomerPositionMixLevel +
				 cur.MarketFeedMixLevel     + cur.MarketWatchMixLevel      +
				 cur.SecurityDetailMixLevel + cur.TradeLookupMixLevel      +
				 cur.TradeOrderMixLevel     + cur.TradeResultMixLevel      +
				 cur.TradeStatusMixLevel    + cur.TradeUpdateMixLevel      == 1000)
				);
	}

	void CheckDefaults(void)
	{
		state.BrokerVolumeMixLevel     = (cur.BrokerVolumeMixLevel     == dft.BrokerVolumeMixLevel);
		state.CustomerPositionMixLevel = (cur.CustomerPositionMixLevel == dft.CustomerPositionMixLevel);
		state.MarketFeedMixLevel       = (cur.MarketFeedMixLevel       == dft.MarketFeedMixLevel);
		state.MarketWatchMixLevel      = (cur.MarketWatchMixLevel      == dft.MarketWatchMixLevel);
		state.SecurityDetailMixLevel   = (cur.SecurityDetailMixLevel   == dft.SecurityDetailMixLevel);
		state.TradeLookupMixLevel      = (cur.TradeLookupMixLevel      == dft.TradeLookupMixLevel);
		state.TradeOrderMixLevel       = (cur.TradeOrderMixLevel       == dft.TradeOrderMixLevel);
		state.TradeResultMixLevel      = (cur.TradeResultMixLevel      == dft.TradeResultMixLevel);
		state.TradeStatusMixLevel      = (cur.TradeStatusMixLevel      == dft.TradeStatusMixLevel);
		state.TradeUpdateMixLevel      = (cur.TradeUpdateMixLevel      == dft.TradeUpdateMixLevel);
	}
};

class CLoaderSettings : public CParametersWithDefaults< struct TLoaderSettings, struct TLoaderSettingsState >
{
public:
	CLoaderSettings( TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
                     TIdent iStartingCustomer, TIdent iCustomerCount,
                     INT32 iScaleFactor, INT32 iDaysOfInitialTrades )
	{
		Initialize();

		cur.iConfiguredCustomerCount = iConfiguredCustomerCount;
		cur.iActiveCustomerCount = iActiveCustomerCount;
		cur.iStartingCustomer = iStartingCustomer;
		cur.iCustomerCount = iCustomerCount;
		cur.iScaleFactor = iScaleFactor;
		cur.iDaysOfInitialTrades = iDaysOfInitialTrades;

		CheckDefaults();
	}

	CLoaderSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		// NOTE: All of these parameters should match the default values hard-
		// coded in src/EGenLoader.cpp via the variable names listed below.
		dft.iConfiguredCustomerCount = 1000;	// iDefaultLoadUnitSize
		dft.iActiveCustomerCount = 1000;		// iDefaultLoadUnitSize
		dft.iStartingCustomer = 1;				// iDefaultStartFromCustomer
		dft.iCustomerCount = 1000;				// iDefaultLoadUnitSize
		dft.iScaleFactor = 500;			    	// iScaleFactor
		dft.iDaysOfInitialTrades = 300;			// iDaysOfInitialTrades
	}

	bool IsValid(void)
	{
		return ( cur.iConfiguredCustomerCount >= 1000 &&
				 cur.iActiveCustomerCount >= 1000 &&
				 (cur.iActiveCustomerCount == cur.iConfiguredCustomerCount) &&
                 (cur.iConfiguredCustomerCount % 1000 == 0) &&
                 cur.iStartingCustomer >= 1 &&
                 cur.iStartingCustomer <= cur.iConfiguredCustomerCount &&
                 (cur.iStartingCustomer % 1000 == 1) &&
                 (cur.iCustomerCount % 1000 == 0) &&
                 cur.iCustomerCount + cur.iStartingCustomer - 1 <= cur.iConfiguredCustomerCount &&
				 cur.iScaleFactor == dft.iScaleFactor &&
				 cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades );
	}

	void CheckDefaults(void)
	{
		state.iConfiguredCustomerCount = true;
		state.iActiveCustomerCount = true;
        state.iStartingCustomer = true;
        state.iCustomerCount = true;
		state.iScaleFactor = (cur.iScaleFactor == dft.iScaleFactor);
		state.iDaysOfInitialTrades = (cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades);
	}
};

class CDriverGlobalSettings : public CParametersWithDefaults< struct TDriverGlobalSettings, struct TDriverGlobalSettingsState >
{
public:
	CDriverGlobalSettings( TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades )
	{
		Initialize();

		cur.iConfiguredCustomerCount = iConfiguredCustomerCount;
		cur.iActiveCustomerCount = iActiveCustomerCount;
		cur.iScaleFactor = iScaleFactor;
		cur.iDaysOfInitialTrades = iDaysOfInitialTrades;

		CheckDefaults();
	}

	CDriverGlobalSettings()
	{
		Initialize();
	}

	void InitializeDefaults(void)
	{
		// NOTE: All of these parameters should match the default values hard-
		// coded in src/EGenLoader.cpp via the variable names listed below,
		// as these are the minimum build (and therefore run) values.
		dft.iConfiguredCustomerCount = 1000;	// iDefaultLoadUnitSize
		dft.iActiveCustomerCount = 1000;		// iDefaultLoadUnitSize
		dft.iScaleFactor = 500;			    	// iScaleFactor
		dft.iDaysOfInitialTrades = 300;			// iDaysOfInitialTrades
	}

	bool IsValid(void)
	{
		return ( cur.iConfiguredCustomerCount >= 1000 &&
				 cur.iActiveCustomerCount >= 1000 &&
				 (cur.iActiveCustomerCount == cur.iConfiguredCustomerCount) &&
                 (cur.iConfiguredCustomerCount % 1000 == 0) &&
				 cur.iScaleFactor == dft.iScaleFactor &&
				 cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades );
	}

	void CheckDefaults(void)
	{
		state.iConfiguredCustomerCount = true;
		state.iActiveCustomerCount = true;
		state.iScaleFactor = (cur.iScaleFactor == dft.iScaleFactor);
		state.iDaysOfInitialTrades = (cur.iDaysOfInitialTrades == dft.iDaysOfInitialTrades);
	}
};

class CDriverCESettings : public CParametersWithoutDefaults< struct TDriverCESettings >
{
public:
	CDriverCESettings( UINT32 UniqueId, RNGSEED TxnMixRNGSeed, RNGSEED TxnInputRNGSeed )
	{
		cur.UniqueId = UniqueId;
		cur.TxnMixRNGSeed = TxnMixRNGSeed;
		cur.TxnInputRNGSeed = TxnInputRNGSeed;
	}

	CDriverCESettings() {} ;

	bool IsValid(void) { return ( true ); }
};

class CDriverCEPartitionSettings : public CParametersWithoutDefaults< struct TDriverCEPartitionSettings >
{
public:
	CDriverCEPartitionSettings( TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent )
	{
		cur.iMyStartingCustomerId = iMyStartingCustomerId;
		cur.iMyCustomerCount = iMyCustomerCount;
		cur.iPartitionPercent = iPartitionPercent;
	}

	CDriverCEPartitionSettings() {} ;

	bool IsValid(void)
	{
		// NOTE: Need CDriverGlobalSettings here to do complete validation.
		return ( (cur.iMyStartingCustomerId % 1000 == 1) &&
                 (cur.iMyCustomerCount % 1000 == 0) &&
                 cur.iPartitionPercent == 50 );
	}
};

class CDriverMEESettings : public CParametersWithoutDefaults< struct TDriverMEESettings >
{
public:
	CDriverMEESettings( UINT32 UniqueId, RNGSEED RNGSeed, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed )
	{
		cur.UniqueId = UniqueId;
		cur.RNGSeed = RNGSeed;
		cur.TickerTapeRNGSeed = TickerTapeRNGSeed;
		cur.TradingFloorRNGSeed = TradingFloorRNGSeed;
	}

	CDriverMEESettings() {} ;

	bool IsValid(void) { return ( true ); }
};

class CDriverDMSettings : public CParametersWithoutDefaults< struct TDriverDMSettings >
{
public:
	CDriverDMSettings( UINT32 UniqueId, RNGSEED RNGSeed )
	{
		cur.UniqueId = UniqueId;
		cur.RNGSeed = RNGSeed;
	}

	CDriverDMSettings() {};

	bool IsValid(void) { return ( true ); }
};

typedef struct TDriverCETxnSettings
{
	CBrokerVolumeSettings		BV_settings;
	CCustomerPositionSettings	CP_settings;
	CMarketWatchSettings		MW_settings;
	CSecurityDetailSettings		SD_settings;
	CTradeLookupSettings		TL_settings;
	CTradeOrderSettings			TO_settings;
	CTradeUpdateSettings		TU_settings;

	CTxnMixGeneratorSettings	TxnMixGenerator_settings;

	bool IsValid()
	{
		return( BV_settings.IsValid() && CP_settings.IsValid() && 
				MW_settings.IsValid() && SD_settings.IsValid() &&
				TL_settings.IsValid() && TO_settings.IsValid() &&
				TU_settings.IsValid() &&
				TxnMixGenerator_settings.IsValid()
				);
	}

} *PDriverCETxnSettings;

}	// namespace TPCE

#endif	//#ifndef DRIVER_PARAM_SETTINGS_H
