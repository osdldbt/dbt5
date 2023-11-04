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

class CCustomLoaderFactory : public CBaseLoaderFactory
{
protected:
	char szConnectStr[iConnectStrLen + 1];

public:
	// Constructor
	CCustomLoaderFactory(char *szLoaderParms) {
		assert(szLoaderParms);

        strncpy(szConnectStr, szLoaderParms, iConnectStrLen);
	}

	// Functions to create loader classes for individual tables.

	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>
			*CreateAccountPermissionLoader() {
		return new CPGSQLAccountPermissionLoad(szConnectStr);
	};

	virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader() {
		return new CPGSQLAddressLoad(szConnectStr);
	};

	virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader() {
		return new CPGSQLBrokerLoad(szConnectStr);
	};

	virtual CBaseLoader<CASH_TRANSACTION_ROW> *CreateCashTransactionLoader() {
		return new CPGSQLCashTransactionLoad(szConnectStr);
	};

	virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader() {
		return new CPGSQLChargeLoad(szConnectStr);
	};

	virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader() {
		return new CPGSQLCommissionRateLoad(szConnectStr);
	};

	virtual CBaseLoader<COMPANY_COMPETITOR_ROW>
			*CreateCompanyCompetitorLoader() {
		return new CPGSQLCompanyCompetitorLoad(szConnectStr);
	};

	virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader() {
		return new CPGSQLCompanyLoad(szConnectStr);
	};

	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader() {
		return new CPGSQLCustomerAccountLoad(szConnectStr);
	};

	virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader() {
		return new CPGSQLCustomerLoad(szConnectStr);
	};

	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader() {
		return new CPGSQLCustomerTaxRateLoad(szConnectStr);
	};

	virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader() {
		return new CPGSQLDailyMarketLoad(szConnectStr);
	};

	virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader() {
		return new CPGSQLExchangeLoad(szConnectStr);
	};

	virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader() {
		return new CPGSQLFinancialLoad(szConnectStr);
	};

	virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader() {
		return new CPGSQLHoldingLoad(szConnectStr);
	};

	virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader() {
		return new CPGSQLHoldingHistoryLoad(szConnectStr);
	};

	virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader() {
		return new CPGSQLHoldingSummaryLoad(szConnectStr);
	};

	virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader() {
		return new CPGSQLIndustryLoad(szConnectStr);
	};

	virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader() {
		return new CPGSQLLastTradeLoad(szConnectStr);
	};

	virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader() {
		return new CPGSQLNewsItemLoad(szConnectStr);
	};

	virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader() {
		return new CPGSQLNewsXRefLoad(szConnectStr);
	};

	virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader() {
		return new CPGSQLSectorLoad(szConnectStr);
	};

	virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader() {
		return new CPGSQLSecurityLoad(szConnectStr);
	};

	virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader() {
		return new CPGSQLSettlementLoad(szConnectStr);
	};

	virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader() {
		return new CPGSQLStatusTypeLoad(szConnectStr);
	};

	virtual CBaseLoader<TAXRATE_ROW> *CreateTaxrateLoader() {
		return new CPGSQLTaxrateLoad(szConnectStr);
	};

	virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader() {
		return new CPGSQLTradeHistoryLoad(szConnectStr);
	};

	virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader() {
		return new CPGSQLTradeLoad(szConnectStr);
	};

	virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader() {
		return new CPGSQLTradeRequestLoad(szConnectStr);
	};

	virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader() {
		return new CPGSQLTradeTypeLoad(szConnectStr);
	};

	virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader() {
		return new CPGSQLWatchItemLoad(szConnectStr);
	};

	virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader() {
		return new CPGSQLWatchListLoad(szConnectStr);
	};

	virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader() {
		return new CPGSQLZipCodeLoad(szConnectStr);
	};
};

} // namespace TPCE

#endif // CUSTOM_LOAD_STDAFX_H
