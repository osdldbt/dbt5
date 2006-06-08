/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
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
