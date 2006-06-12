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
 * - Sergey Vasilevskiy
 */

/*
*	Base interface for a loader class factory.
*	This class instantiates particular table loader classes.
*/

#ifndef BASE_LOADER_FACTORY_H
#define BASE_LOADER_FACTORY_H

namespace TPCE
{

class CBaseLoaderFactory
{
	
public:	

	// Virtual destructor
	virtual ~CBaseLoaderFactory() {};

	// Functions to create loader classes for individual tables.
	// Should be defined in a subclass according to the subclass load type.
	
	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*	CreateAccountPermissionLoader() = 0;
	virtual CBaseLoader<ADDRESS_ROW>*				CreateAddressLoader() = 0;
	virtual CBaseLoader<BROKER_ROW>*				CreateBrokerLoader() = 0;
	virtual CBaseLoader<CASH_TRANSACTION_ROW>*		CreateCashTransactionLoader() = 0;
	virtual CBaseLoader<CHARGE_ROW>*				CreateChargeLoader() = 0;
	virtual CBaseLoader<COMMISSION_RATE_ROW>*		CreateCommissionRateLoader() = 0;
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*	CreateCompanyCompetitorLoader() = 0;
	virtual CBaseLoader<COMPANY_ROW>*				CreateCompanyLoader() = 0;
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*		CreateCustomerAccountLoader() = 0;
	virtual CBaseLoader<CUSTOMER_ROW>*				CreateCustomerLoader() = 0;
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*		CreateCustomerTaxrateLoader() = 0;
	virtual CBaseLoader<DAILY_MARKET_ROW>*			CreateDailyMarketLoader() = 0;
	virtual CBaseLoader<EXCHANGE_ROW>*				CreateExchangeLoader() = 0;
	virtual CBaseLoader<FINANCIAL_ROW>*				CreateFinancialLoader() = 0;
	virtual CBaseLoader<HOLDING_ROW>*				CreateHoldingLoader() = 0;
	virtual CBaseLoader<HOLDING_HISTORY_ROW>*		CreateHoldingHistoryLoader() = 0;
	virtual CBaseLoader<HOLDING_SUMMARY_ROW>*		CreateHoldingSummaryLoader() = 0;
	virtual CBaseLoader<INDUSTRY_ROW>*				CreateIndustryLoader() = 0;
	virtual CBaseLoader<LAST_TRADE_ROW>*			CreateLastTradeLoader() = 0;
	virtual CBaseLoader<NEWS_ITEM_ROW>*				CreateNewsItemLoader() = 0;
	virtual CBaseLoader<NEWS_XREF_ROW>*				CreateNewsXRefLoader() = 0;
	virtual CBaseLoader<SECTOR_ROW>*				CreateSectorLoader() = 0;
	virtual CBaseLoader<SECURITY_ROW>*				CreateSecurityLoader() = 0;
	virtual CBaseLoader<SETTLEMENT_ROW>*			CreateSettlementLoader() = 0;
	virtual CBaseLoader<STATUS_TYPE_ROW>*			CreateStatusTypeLoader() = 0;
	virtual CBaseLoader<TAXRATE_ROW>*				CreateTaxrateLoader() = 0;
	virtual CBaseLoader<TRADE_HISTORY_ROW>*			CreateTradeHistoryLoader() = 0;
	virtual CBaseLoader<TRADE_ROW>*					CreateTradeLoader() = 0;
	virtual CBaseLoader<TRADE_REQUEST_ROW>*			CreateTradeRequestLoader() = 0;
	virtual CBaseLoader<TRADE_TYPE_ROW>*			CreateTradeTypeLoader() = 0;
	virtual CBaseLoader<WATCH_ITEM_ROW>*			CreateWatchItemLoader() = 0;
	virtual CBaseLoader<WATCH_LIST_ROW>*			CreateWatchListLoader() = 0;	
	virtual CBaseLoader<ZIP_CODE_ROW>*				CreateZipCodeLoader() = 0;
};

}	// namespace TPCE

#endif	// #ifndef BASE_LOADER_FACTORY_H
