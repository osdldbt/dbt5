/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	loader class factory for PostgreSQL.
*	This class instantiates particular table loader classes.
*/
#ifndef PGSQL_LOADER_FACTORY_H
#define PGSQL_LOADER_FACTORY_H

namespace TPCE
{

class CPGSQLLoaderFactory : public CBaseLoaderFactory
{
	char		m_szHost[iMaxPGHost];		// host name
	char		m_szDBName[iMaxPGDBName];	// database name
	char		m_szPostmasterPort[iMaxPGPort]; // Postmaster port
	
public:	
	// Constructor

	//CPGSQLLoaderFactory(char *szServer, char *szDatabase)
	CPGSQLLoaderFactory(char *szHost, char *szDBName, char *szPostmasterPort)
	{
		assert(szHost);
		assert(szDBName);
		assert(szPostmasterPort);

		strncpy(m_szHost, szHost, sizeof(m_szHost));
		strncpy(m_szDBName, szDBName, sizeof(m_szDBName));
		strncpy(m_szPostmasterPort, szPostmasterPort, sizeof(m_szPostmasterPort));
	}

	// Functions to create loader classes for individual tables.	
	
	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*	CreateAccountPermissionLoader() 
	{
		return new CPGSQLAccountPermissionLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<ADDRESS_ROW>*				CreateAddressLoader() 
	{
		return new CPGSQLAddressLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<BROKER_ROW>*				CreateBrokerLoader() 
	{
		return new CPGSQLBrokerLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CASH_TRANSACTION_ROW>*		CreateCashTransactionLoader() 
	{
		return new CPGSQLCashTransactionLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CHARGE_ROW>*				CreateChargeLoader() 
	{
		return new CPGSQLChargeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMMISSION_RATE_ROW>*		CreateCommissionRateLoader() 
	{
		return new CPGSQLCommissionRateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*	CreateCompanyCompetitorLoader() 
	{
		return new CPGSQLCompanyCompetitorLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMPANY_ROW>*				CreateCompanyLoader() 
	{
		return new CPGSQLCompanyLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*		CreateCustomerAccountLoader() 
	{
		return new CPGSQLCustomerAccountLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_ROW>*				CreateCustomerLoader() 
	{
		return new CPGSQLCustomerLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*		CreateCustomerTaxrateLoader() 
	{
		return new CPGSQLCustomerTaxRateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<DAILY_MARKET_ROW>*			CreateDailyMarketLoader() 
	{
		return new CPGSQLDailyMarketLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<EXCHANGE_ROW>*				CreateExchangeLoader() 
	{
		return new CPGSQLExchangeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<FINANCIAL_ROW>*				CreateFinancialLoader() 
	{
		return new CPGSQLFinancialLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_ROW>*				CreateHoldingLoader() 
	{
		return new CPGSQLHoldingLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW>*		CreateHoldingHistoryLoader() 
	{
		return new CPGSQLHoldingHistoryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW>*			CreateHoldingSummaryLoader() 
	{
		return new CPGSQLHoldingSummaryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<INDUSTRY_ROW>*				CreateIndustryLoader() 
	{
		return new CPGSQLIndustryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<LAST_TRADE_ROW>*			CreateLastTradeLoader() 
	{
		return new CPGSQLLastTradeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<NEWS_ITEM_ROW>*				CreateNewsItemLoader() 
	{
		return new CPGSQLNewsItemLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<NEWS_XREF_ROW>*				CreateNewsXRefLoader() 
	{
		return new CPGSQLNewsXRefLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SECTOR_ROW>*				CreateSectorLoader() 
	{
		return new CPGSQLSectorLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SECURITY_ROW>*				CreateSecurityLoader() 
	{
		return new CPGSQLSecurityLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SETTLEMENT_ROW>*			CreateSettlementLoader() 
	{
		return new CPGSQLSettlementLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<STATUS_TYPE_ROW>*			CreateStatusTypeLoader() 
	{
		return new CPGSQLStatusTypeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TAXRATE_ROW>*				CreateTaxrateLoader() 
	{
		return new CPGSQLTaxrateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW>*			CreateTradeHistoryLoader() 
	{
		return new CPGSQLTradeHistoryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_ROW>*					CreateTradeLoader() 
	{
		return new CPGSQLTradeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW>*			CreateTradeRequestLoader() 
	{
		return new CPGSQLTradeRequestLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_TYPE_ROW>*			CreateTradeTypeLoader() 
	{
		return new CPGSQLTradeTypeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<WATCH_ITEM_ROW>*			CreateWatchItemLoader() 
	{
		return new CPGSQLWatchItemLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<WATCH_LIST_ROW>*			CreateWatchListLoader() 
	{
		return new CPGSQLWatchListLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<ZIP_CODE_ROW>*				CreateZipCodeLoader() 
	{
		return new CPGSQLZipCodeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
};

}	// namespace TPCE

#endif //PGSQL_LOADER_FACTORY_H
