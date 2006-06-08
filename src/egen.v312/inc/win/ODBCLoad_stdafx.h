/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef ODBCLOAD_STDAFX_H
#define ODBCLOAD_STDAFX_H

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers

#include <windows.h>

// ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>

#include "../EGenUtilities_stdafx.h"
#include "../Table_Defs.h"
#include "../EGenBaseLoader_stdafx.h"
#include "DBLoader.h"
#include "ODBCAccountPermissionLoad.h"
#include "ODBCAddressLoad.h"
#include "ODBCBrokerLoad.h"
#include "ODBCCashTransactionLoad.h"
#include "ODBCChargeLoad.h"
#include "ODBCCommissionRateLoad.h"
#include "ODBCCompanyLoad.h"
#include "ODBCCompanyCompetitorLoad.h"
#include "ODBCCustomerLoad.h"
#include "ODBCCustomerAccountLoad.h"
#include "ODBCCustomerTaxrateLoad.h"
#include "ODBCDailyMarketLoad.h"
#include "ODBCExchangeLoad.h"
#include "ODBCFinancialLoad.h"
#include "ODBCHoldingLoad.h"
#include "ODBCHoldingHistoryLoad.h"
#include "ODBCHoldingSummaryLoad.h"
#include "ODBCIndustryLoad.h"
#include "ODBCLastTradeLoad.h"
#include "ODBCNewsItemLoad.h"
#include "ODBCNewsXRefLoad.h"
#include "ODBCSectorLoad.h"
#include "ODBCSecurityLoad.h"
#include "ODBCSettlementLoad.h"
#include "ODBCStatusTypeLoad.h"
#include "ODBCTaxrateLoad.h"
#include "ODBCTradeLoad.h"
#include "ODBCTradeHistoryLoad.h"
#include "ODBCTradeRequestLoad.h"
#include "ODBCTradeTypeLoad.h"
#include "ODBCWatchItemLoad.h"
#include "ODBCWatchListLoad.h"
#include "ODBCZipCodeLoad.h"
#include "ODBCLoaderFactory.h"

#endif	// #ifndef ODBCLOAD_STDAFX_H
