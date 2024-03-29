/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
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
 * - 2006 Rilson Nascimento
 * - 2010 Mark Wong <markwkm@postgresql.org>
 */

//
// loader class factory for PostgreSQL.
// This class instantiates particular table loader classes.
//

#ifndef CUSTOM_LOAD_STDAFX_H
#define CUSTOM_LOAD_STDAFX_H

#ifdef PGSQL
#include "pgsql/PGSQLLoad.h"
#endif // PGSQL

namespace TPCE
{

class CCustomLoaderFactory: public CBaseLoaderFactory
{
protected:
	char szConnectStr[iConnectStrLen + 1];

public:
	// Constructor
	CCustomLoaderFactory(char *szLoaderParms)
	{
		assert(szLoaderParms);

		strncpy(szConnectStr, szLoaderParms, iConnectStrLen);
	}

	// Destructor
	~CCustomLoaderFactory(){};

	// Functions to create loader classes for individual tables.

	CBaseLoader<ACCOUNT_PERMISSION_ROW> *
	CreateAccountPermissionLoader()
	{
		return new CPGSQLAccountPermissionLoad(szConnectStr);
	};

	CBaseLoader<ADDRESS_ROW> *
	CreateAddressLoader()
	{
		return new CPGSQLAddressLoad(szConnectStr);
	};

	CBaseLoader<BROKER_ROW> *
	CreateBrokerLoader()
	{
		return new CPGSQLBrokerLoad(szConnectStr);
	};

	CBaseLoader<CASH_TRANSACTION_ROW> *
	CreateCashTransactionLoader()
	{
		return new CPGSQLCashTransactionLoad(szConnectStr);
	};

	CBaseLoader<CHARGE_ROW> *
	CreateChargeLoader()
	{
		return new CPGSQLChargeLoad(szConnectStr);
	};

	CBaseLoader<COMMISSION_RATE_ROW> *
	CreateCommissionRateLoader()
	{
		return new CPGSQLCommissionRateLoad(szConnectStr);
	};

	CBaseLoader<COMPANY_COMPETITOR_ROW> *
	CreateCompanyCompetitorLoader()
	{
		return new CPGSQLCompanyCompetitorLoad(szConnectStr);
	};

	CBaseLoader<COMPANY_ROW> *
	CreateCompanyLoader()
	{
		return new CPGSQLCompanyLoad(szConnectStr);
	};

	CBaseLoader<CUSTOMER_ACCOUNT_ROW> *
	CreateCustomerAccountLoader()
	{
		return new CPGSQLCustomerAccountLoad(szConnectStr);
	};

	CBaseLoader<CUSTOMER_ROW> *
	CreateCustomerLoader()
	{
		return new CPGSQLCustomerLoad(szConnectStr);
	};

	CBaseLoader<CUSTOMER_TAXRATE_ROW> *
	CreateCustomerTaxrateLoader()
	{
		return new CPGSQLCustomerTaxRateLoad(szConnectStr);
	};

	CBaseLoader<DAILY_MARKET_ROW> *
	CreateDailyMarketLoader()
	{
		return new CPGSQLDailyMarketLoad(szConnectStr);
	};

	CBaseLoader<EXCHANGE_ROW> *
	CreateExchangeLoader()
	{
		return new CPGSQLExchangeLoad(szConnectStr);
	};

	CBaseLoader<FINANCIAL_ROW> *
	CreateFinancialLoader()
	{
		return new CPGSQLFinancialLoad(szConnectStr);
	};

	CBaseLoader<HOLDING_ROW> *
	CreateHoldingLoader()
	{
		return new CPGSQLHoldingLoad(szConnectStr);
	};

	CBaseLoader<HOLDING_HISTORY_ROW> *
	CreateHoldingHistoryLoader()
	{
		return new CPGSQLHoldingHistoryLoad(szConnectStr);
	};

	CBaseLoader<HOLDING_SUMMARY_ROW> *
	CreateHoldingSummaryLoader()
	{
		return new CPGSQLHoldingSummaryLoad(szConnectStr);
	};

	CBaseLoader<INDUSTRY_ROW> *
	CreateIndustryLoader()
	{
		return new CPGSQLIndustryLoad(szConnectStr);
	};

	CBaseLoader<LAST_TRADE_ROW> *
	CreateLastTradeLoader()
	{
		return new CPGSQLLastTradeLoad(szConnectStr);
	};

	CBaseLoader<NEWS_ITEM_ROW> *
	CreateNewsItemLoader()
	{
		return new CPGSQLNewsItemLoad(szConnectStr);
	};

	CBaseLoader<NEWS_XREF_ROW> *
	CreateNewsXRefLoader()
	{
		return new CPGSQLNewsXRefLoad(szConnectStr);
	};

	CBaseLoader<SECTOR_ROW> *
	CreateSectorLoader()
	{
		return new CPGSQLSectorLoad(szConnectStr);
	};

	CBaseLoader<SECURITY_ROW> *
	CreateSecurityLoader()
	{
		return new CPGSQLSecurityLoad(szConnectStr);
	};

	CBaseLoader<SETTLEMENT_ROW> *
	CreateSettlementLoader()
	{
		return new CPGSQLSettlementLoad(szConnectStr);
	};

	CBaseLoader<STATUS_TYPE_ROW> *
	CreateStatusTypeLoader()
	{
		return new CPGSQLStatusTypeLoad(szConnectStr);
	};

	CBaseLoader<TAX_RATE_ROW> *
	CreateTaxRateLoader()
	{
		return new CPGSQLTaxrateLoad(szConnectStr);
	};

	CBaseLoader<TRADE_HISTORY_ROW> *
	CreateTradeHistoryLoader()
	{
		return new CPGSQLTradeHistoryLoad(szConnectStr);
	};

	CBaseLoader<TRADE_ROW> *
	CreateTradeLoader()
	{
		return new CPGSQLTradeLoad(szConnectStr);
	};

	CBaseLoader<TRADE_REQUEST_ROW> *
	CreateTradeRequestLoader()
	{
		return new CPGSQLTradeRequestLoad(szConnectStr);
	};

	CBaseLoader<TRADE_TYPE_ROW> *
	CreateTradeTypeLoader()
	{
		return new CPGSQLTradeTypeLoad(szConnectStr);
	};

	CBaseLoader<WATCH_ITEM_ROW> *
	CreateWatchItemLoader()
	{
		return new CPGSQLWatchItemLoad(szConnectStr);
	};

	CBaseLoader<WATCH_LIST_ROW> *
	CreateWatchListLoader()
	{
		return new CPGSQLWatchListLoad(szConnectStr);
	};

	CBaseLoader<ZIP_CODE_ROW> *
	CreateZipCodeLoader()
	{
		return new CPGSQLZipCodeLoad(szConnectStr);
	};
};

} // namespace TPCE

#endif // CUSTOM_LOAD_STDAFX_H
