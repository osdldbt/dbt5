/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	ODBC loader class factory for MS SQL Server.
*	This class instantiates particular table loader classes.
*/
#ifndef ODBC_LOADER_FACTORY_H
#define ODBC_LOADER_FACTORY_H

namespace TPCE
{

class CODBCLoaderFactory : public CBaseLoaderFactory
{
	char		m_szServer[iMaxHostname];	// database server name
	char		m_szDB[iMaxDBName];		// database name
	
public:	
	// Constructor
	CODBCLoaderFactory(char *szServer, char *szDatabase)		
	{
		assert(szServer);
		assert(szDatabase);

		strncpy(m_szServer, szServer, sizeof(m_szServer));

		strncpy(m_szDB, szDatabase, sizeof(m_szDB));
	}

	// Functions to create loader classes for individual tables.	
	
	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*	CreateAccountPermissionLoader() 
	{
		return new CODBCAccountPermissionLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<ADDRESS_ROW>*				CreateAddressLoader() 
	{
		return new CODBCAddressLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<BROKER_ROW>*				CreateBrokerLoader() 
	{
		return new CODBCBrokerLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<CASH_TRANSACTION_ROW>*		CreateCashTransactionLoader() 
	{
		return new CODBCCashTransactionLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<CHARGE_ROW>*				CreateChargeLoader() 
	{
		return new CODBCChargeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<COMMISSION_RATE_ROW>*		CreateCommissionRateLoader() 
	{
		return new CODBCCommissionRateLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*	CreateCompanyCompetitorLoader() 
	{
		return new CODBCCompanyCompetitorLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<COMPANY_ROW>*				CreateCompanyLoader() 
	{
		return new CODBCCompanyLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*		CreateCustomerAccountLoader() 
	{
		return new CODBCCustomerAccountLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<CUSTOMER_ROW>*				CreateCustomerLoader() 
	{
		return new CODBCCustomerLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*		CreateCustomerTaxrateLoader() 
	{
		return new CODBCCustomerTaxRateLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<DAILY_MARKET_ROW>*			CreateDailyMarketLoader() 
	{
		return new CODBCDailyMarketLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<EXCHANGE_ROW>*				CreateExchangeLoader() 
	{
		return new CODBCExchangeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<FINANCIAL_ROW>*				CreateFinancialLoader() 
	{
		return new CODBCFinancialLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<HOLDING_ROW>*				CreateHoldingLoader() 
	{
		return new CODBCHoldingLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW>*		CreateHoldingHistoryLoader() 
	{
		return new CODBCHoldingHistoryLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW>*			CreateHoldingSummaryLoader() 
	{
		return new CODBCHoldingSummaryLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<INDUSTRY_ROW>*				CreateIndustryLoader() 
	{
		return new CODBCIndustryLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<LAST_TRADE_ROW>*			CreateLastTradeLoader() 
	{
		return new CODBCLastTradeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<NEWS_ITEM_ROW>*				CreateNewsItemLoader() 
	{
		return new CODBCNewsItemLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<NEWS_XREF_ROW>*				CreateNewsXRefLoader() 
	{
		return new CODBCNewsXRefLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<SECTOR_ROW>*				CreateSectorLoader() 
	{
		return new CODBCSectorLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<SECURITY_ROW>*				CreateSecurityLoader() 
	{
		return new CODBCSecurityLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<SETTLEMENT_ROW>*			CreateSettlementLoader() 
	{
		return new CODBCSettlementLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<STATUS_TYPE_ROW>*			CreateStatusTypeLoader() 
	{
		return new CODBCStatusTypeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<TAXRATE_ROW>*				CreateTaxrateLoader() 
	{
		return new CODBCTaxrateLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW>*			CreateTradeHistoryLoader() 
	{
		return new CODBCTradeHistoryLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<TRADE_ROW>*					CreateTradeLoader() 
	{
		return new CODBCTradeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW>*			CreateTradeRequestLoader() 
	{
		return new CODBCTradeRequestLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<TRADE_TYPE_ROW>*			CreateTradeTypeLoader() 
	{
		return new CODBCTradeTypeLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<WATCH_ITEM_ROW>*			CreateWatchItemLoader() 
	{
		return new CODBCWatchItemLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<WATCH_LIST_ROW>*			CreateWatchListLoader() 
	{
		return new CODBCWatchListLoad(m_szServer, m_szDB);
	};
	virtual CBaseLoader<ZIP_CODE_ROW>*				CreateZipCodeLoader() 
	{
		return new CODBCZipCodeLoad(m_szServer, m_szDB);
	};
};

}	// namespace TPCE

#endif //ODBC_LOADER_FACTORY_H
