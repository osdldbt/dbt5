/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef EGEN_TABLES_STDAFX_H
#define EGEN_TABLES_STDAFX_H

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
#include <assert.h>
#include <math.h>
#include <fstream>
#include <set>
#include <map>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <queue>

// TODO: reference additional headers your program requires here
using namespace std;

//Define unsigned type for convenience
typedef unsigned int UINT;

namespace TPCE
{

static const int iDefaultStartFromCustomer = 1;
const int	iBrokersDiv = 10;	// by what number to divide the customer count to get the broker count
const UINT	iDefaultLoadUnitSize = 1000;	// number of customers in default load unit

}	// namespace TPCE

#include "EGenUtilities_stdafx.h"
#include "InputFile.h"
#include "InputFileNoWeight.h"
#include "FlatFile.h"
#include "Table_Defs.h"
#include "InputFlatFilesDeclarations.h"
#include "SecurityFile.h"
#include "CompanyFile.h"
#include "CompanyCompetitorFile.h"
#include "InputFlatFilesStructure.h"
#include "TableTemplate.h"
#include "Person.h"
#include "CustomerSelection.h"
#include "CustomerTable.h"
#include "CompanyTable.h"	//must be before Address and Financial tables
#include "FinancialTable.h"
#include "AddressTable.h"
#include "CustomerAccountsAndPermissionsTable.h"
#include "CustomerTaxratesTable.h"
#include "HoldingsAndTradesTable.h"
#include "WatchListsAndItemsTable.h"
#include "SecurityTable.h"
#include "DailyMarketTable.h"
#include "Brokers.h"
#include "SectorTable.h"
#include "ChargeTable.h"
#include "ExchangeTable.h"
#include "CommissionRateTable.h"
#include "IndustryTable.h"
#include "StatusTypeTable.h"
#include "TaxrateTable.h"
#include "TradeTypeTable.h"
#include "CompanyCompetitorTable.h"
#include "ZipCodeTable.h"
#include "NewsItemAndXRefTable.h"
#include "MEESecurity.h"	// must be before LastTradeTable.h
#include "LastTradeTable.h"
#include "TradeGen.h"

#endif	// #ifndef EGEN_TABLES_STDAFX_H
