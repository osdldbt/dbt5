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
	// Logger instance
	CBaseLogger*				m_pLogger;
	// Parameter instance
	CLoaderSettings				m_LoaderSettings;

public:
	CGenerateAndLoad(CInputFiles				inputFiles, 
					TIdent						iCustomerCount, 
					TIdent						iStartFromCustomer,
					TIdent						iTotalCustomers,
					int							iLoadUnitSize,
					int							iScaleFactor,
					int							iDaysOfInitialTrades,
					CBaseLoaderFactory*			pLoaderFactory,
					CBaseLogger*				pLogger,
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
