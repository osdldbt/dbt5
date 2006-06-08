/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	This file contains a class that acts as a client to the table
*	generation classes (EGenTables) and to the loader classes (EGenBaseLoader).
*	It provides routines for generating and loading the table data or its subset.
*/

#ifndef EGEN_GENERATE_AND_LOAD_H
#define EGEN_GENERATE_AND_LOAD_H


namespace TPCE
{

class CGenerateAndLoad
{
	// Structure containing references to input files loaded into memory
	CInputFiles					m_inputFiles;
	// Ordinal position (1-based) of the first customer in the sequence
	TIdent						m_iStartFromCustomer;
	// The number of customers to generate from the starting position
	TIdent						m_iCustomerCount;
	// Total number of customers in the database
	TIdent						m_iTotalCustomers;
	// Number of customers in one load unit for generating initial trades
	int							m_iLoadUnitSize;
	// Number of customers per 1 tpsE
	int							m_iScaleFactor;
	// Time period for which to generate initial trades
	int							m_iHoursOfInitialTrades;
	// External loader factory to create table loaders
	CBaseLoaderFactory*			m_pLoaderFactory;
	// External class used to output load progress
	CGenerateAndLoadBaseOutput*	m_pOutput;
	// Input flat file directory for tables loaded via flat files
	char						m_szInDir[iMaxPath];	

public:
	CGenerateAndLoad(CInputFiles				inputFiles, 
					TIdent						iCustomerCount, 
					TIdent						iStartFromCustomer,
					TIdent						iTotalCustomers,
					int							iLoadUnitSize,
					int							iScaleFactor,
					int							iHoursOfInitialTrades,
					CBaseLoaderFactory*			pLoaderFactory,
					CGenerateAndLoadBaseOutput* pOutput,
					char*						szInDir);	// directory for input flat files

	void GenerateAndLoadAddress();	
	void GenerateAndLoadCharge();
	void GenerateAndLoadCommissionRate();
	void GenerateAndLoadCompanyCompetitor();	
	void GenerateAndLoadCompany();
	void GenerateAndLoadCustomerAccountAndAccountPermission();
	void GenerateAndLoadCustomer();
	void GenerateAndLoadCustomerTaxrate();
	void GenerateAndLoadDailyMarket();
	void GenerateAndLoadExchange();
	void GenerateAndLoadFinancial();
	void GenerateAndLoadHoldingAndTrade();
	void GenerateAndLoadIndustry();
	void GenerateAndLoadLastTrade();
	void GenerateAndLoadNewsItemAndNewsXRef();
	void GenerateAndLoadSector();
	void GenerateAndLoadSecurity();
	void GenerateAndLoadStatusType();
	void GenerateAndLoadTaxrate();
	void GenerateAndLoadTradeType();
	void GenerateAndLoadWatchListAndWatchItem();
	void GenerateAndLoadZipCode();

	// Generate and load all tables whose size is independent of the number of customers.
	void GenerateAndLoadFixedTables();

	// Generate and load all tables (except BROKER) that scale with
	// the number of customers, but do not grow during runtime.
	void GenerateAndLoadScalingTables();

	// Generate and load trade related tables and BROKER.
	void GenerateAndLoadGrowingTables();
};

}	// namespace TPCE

#endif //EGEN_GENERATE_AND_LOAD_H
