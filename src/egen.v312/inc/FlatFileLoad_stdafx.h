/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef FLAT_FILE_LOAD_STDAFX_H
#define FLAT_FILE_LOAD_STDAFX_H

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers

#include <iostream>
#include <fstream>
#include <cstdio>

#include "EGenUtilities_stdafx.h"
#include "Table_Defs.h"
#include "EGenBaseLoader_stdafx.h"
#include "FlatFileLoader.h"
#include "FlatAccountPermissionLoad.h"
#include "FlatAddressLoad.h"
#include "FlatBrokerLoad.h"
#include "FlatCashTransactionLoad.h"
#include "FlatChargeLoad.h"
#include "FlatCommissionRateLoad.h"
#include "FlatCompanyLoad.h"
#include "FlatCompanyCompetitorLoad.h"
#include "FlatCustomerLoad.h"
#include "FlatCustomerAccountLoad.h"
#include "FlatCustomerTaxrateLoad.h"
#include "FlatDailyMarketLoad.h"
#include "FlatExchangeLoad.h"
#include "FlatFinancialLoad.h"
#include "FlatHoldingLoad.h"
#include "FlatHoldingHistoryLoad.h"
#include "FlatHoldingSummaryLoad.h"
#include "FlatIndustryLoad.h"
#include "FlatLastTradeLoad.h"
#include "FlatNewsItemLoad.h"
#include "FlatNewsXRefLoad.h"
#include "FlatSectorLoad.h"
#include "FlatSecurityLoad.h"
#include "FlatSettlementLoad.h"
#include "FlatStatusTypeLoad.h"
#include "FlatTaxrateLoad.h"
#include "FlatTradeLoad.h"
#include "FlatTradeHistoryLoad.h"
#include "FlatTradeRequestLoad.h"
#include "FlatTradeTypeLoad.h"
#include "FlatWatchItemLoad.h"
#include "FlatWatchListLoad.h"
#include "FlatZipCodeLoad.h"
#include "FlatLoaderFactory.h"

#endif	// #ifndef FLAT_FILE_LOAD_STDAFX_H
