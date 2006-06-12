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

#include "../inc/EGenGenerateAndLoad_stdafx.h"
#include "../inc/DriverParamSettings.h"

using namespace TPCE;

//
//	Constructor.
//
CGenerateAndLoad::CGenerateAndLoad(CInputFiles					inputFiles, 
								  TIdent						iCustomerCount, 
								  TIdent						iStartFromCustomer,
								  TIdent						iTotalCustomers,
								  int							iLoadUnitSize,
								  int							iScaleFactor,
								  int							iDaysOfInitialTrades,
								  CBaseLoaderFactory*			pLoaderFactory,
								  CBaseLogger*				pLogger,
								  CGenerateAndLoadBaseOutput*	pOutput,
								  char*							szInDir)
: m_inputFiles(inputFiles)
, m_iCustomerCount(iCustomerCount)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iTotalCustomers(iTotalCustomers)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iScaleFactor(iScaleFactor)
, m_iHoursOfInitialTrades(iDaysOfInitialTrades*HoursPerWorkDay)
, m_pLoaderFactory(pLoaderFactory)
, m_pOutput(pOutput)
, m_LoaderSettings(iTotalCustomers, iTotalCustomers, iCustomerCount, iStartFromCustomer, iScaleFactor, iDaysOfInitialTrades )
, m_pLogger(pLogger)
{	
	// Copy input flat file directory needed for tables loaded from flat files.
	strncpy( m_szInDir, szInDir, sizeof(m_szInDir)-1);	

	// Log Parameters
	m_pLogger->SendToLogger(m_LoaderSettings);
};

// ADDRESS
void CGenerateAndLoad::GenerateAndLoadAddress()
{
	bool						bRet;
	CAddressTable				Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer,
									  // do not generate exchange and company addresses
									  // if the starting customer is not 1
									  m_iStartFromCustomer != iDefaultStartFromCustomer);
	CBaseLoader<ADDRESS_ROW>*	pLoad = m_pLoaderFactory->CreateAddressLoader();
	INT64						iCnt=0;
	
	m_pOutput->OutputStart("Generating ADDRESS table...");	

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0)
		{	//output progress
			m_pOutput->OutputProgress(".");			
		}

	} while (bRet);

	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// CHARGE
void CGenerateAndLoad::GenerateAndLoadCharge()
{
	bool						bEndOfFile;
	CChargeTable				Table( m_szInDir );
	CBaseLoader<CHARGE_ROW>*	pLoad = m_pLoaderFactory->CreateChargeLoader();

	m_pOutput->OutputStart("Generating CHARGE table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// COMMISSION_RATE
void CGenerateAndLoad::GenerateAndLoadCommissionRate()
{
	bool								bEndOfFile;
	CCommissionRateTable				Table( m_szInDir );
	CBaseLoader<COMMISSION_RATE_ROW>*	pLoad = m_pLoaderFactory->CreateCommissionRateLoader();

	m_pOutput->OutputStart("Generating COMMISSION_RATE table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// COMPANY
void CGenerateAndLoad::GenerateAndLoadCompany()
{
	bool							bRet;
	CCompanyTable					Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<COMPANY_ROW>*		pLoad = m_pLoaderFactory->CreateCompanyLoader();	
	
	m_pOutput->OutputStart("Generating COMPANY table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());
		
	} while (bRet);

	pLoad->FinishLoad();	//commit
	
	delete pLoad;
	
	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// COMPANY_COMPETITOR
void CGenerateAndLoad::GenerateAndLoadCompanyCompetitor()
{
	bool									bRet;
	CCompanyCompetitorTable					Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<COMPANY_COMPETITOR_ROW>*	pLoad = m_pLoaderFactory->CreateCompanyCompetitorLoader();
	
	m_pOutput->OutputStart("Generating COMPANY_COMPETITOR table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());
		
	} while (bRet);

	pLoad->FinishLoad();	//commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// CUSTOMER
void CGenerateAndLoad::GenerateAndLoadCustomer()
{
	bool						bRet;
	CCustomerTable				Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_ROW>*	pLoad = m_pLoaderFactory->CreateCustomerLoader();
	INT64						iCnt=0;
	
	m_pOutput->OutputStart("Generating CUSTOMER table...");

	pLoad->Init();

	do 
	{
		bRet = Table.GenerateNextRecord();
		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress
		}		

	} while (bRet);

	pLoad->FinishLoad();	//commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// CUSTOMER_ACCOUNT, ACCOUNT_PERMISSION
void CGenerateAndLoad::GenerateAndLoadCustomerAccountAndAccountPermission()
{
	bool									bRet;
	CCustomerAccountsAndPermissionsTable	Table(m_inputFiles, m_iLoadUnitSize, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_ACCOUNT_ROW>*		pCALoad = m_pLoaderFactory->CreateCustomerAccountLoader();
	CBaseLoader<ACCOUNT_PERMISSION_ROW>*	pAPLoad = m_pLoaderFactory->CreateAccountPermissionLoader();
	INT64									iCnt=0;
	int										i;
	
	m_pOutput->OutputStart("Generating CUSTOMER_ACCOUNT table and ACCOUNT_PERMISSION table...");

	pCALoad->Init();
	pAPLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		pCALoad->WriteNextRecord(Table.GetCARow());

		for(i=0; i<Table.GetCAPermsCount(); ++i)
		{
			pAPLoad->WriteNextRecord(Table.GetAPRow(i));
			
		}

		if (++iCnt % 10000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress
		}

		// Commit rows every so often
		if (iCnt % 10000 == 0)
		{
			pCALoad->Commit();
			pAPLoad->Commit();
		}

	} while (bRet);
	pCALoad->FinishLoad();	//commit
	pAPLoad->FinishLoad();	//commit
	delete pCALoad;
	delete pAPLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// CUSTOMER_TAXRATE
void CGenerateAndLoad::GenerateAndLoadCustomerTaxrate()
{
	bool								bRet;
	CCustomerTaxratesTable				Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<CUSTOMER_TAXRATE_ROW>*	pLoad = m_pLoaderFactory->CreateCustomerTaxrateLoader();
	INT64								iCnt=0;
	int									i;
	
	m_pOutput->OutputStart("Generating CUSTOMER_TAX_RATE table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		for (i=0; i<Table.GetTaxRatesCount(); ++i)
		{
			pLoad->WriteNextRecord(Table.GetRow(i));

			if (++iCnt % 20000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}			
		}
	} while (bRet);

	pLoad->FinishLoad();	//commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// DAILY_MARKET
void CGenerateAndLoad::GenerateAndLoadDailyMarket()
{
	bool							bRet;
	CDailyMarketTable				Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<DAILY_MARKET_ROW>*	pLoad = m_pLoaderFactory->CreateDailyMarketLoader();
	INT64							iCnt=0;

	m_pOutput->OutputStart("Generating DAILY_MARKET table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();		
		
		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress		
		}

		if (iCnt % 6525 == 0)
		{			
			pLoad->Commit();	// commit rows every 5 securities (1305 rows/security)
		}

	} while (bRet);
	
	pLoad->FinishLoad();	//commit

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// EXCHANGE
void CGenerateAndLoad::GenerateAndLoadExchange()
{
	bool						bEndOfFile;
	CExchangeTable				Table( m_szInDir );
	CBaseLoader<EXCHANGE_ROW>*	pLoad = m_pLoaderFactory->CreateExchangeLoader();

	m_pOutput->OutputStart("Generating EXCHANGE table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// FINANCIAL
void CGenerateAndLoad::GenerateAndLoadFinancial()
{
	bool							bRet;
	CFinancialTable					Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);	
	CBaseLoader<FINANCIAL_ROW>*		pLoad = m_pLoaderFactory->CreateFinancialLoader();
	INT64							iCnt=0;	

	m_pOutput->OutputStart("Generating FINANCIAL table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();
				
		pLoad->WriteNextRecord(Table.GetRow());

		if (++iCnt % 20000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress		
		}

		if (iCnt % 5000 == 0)
		{			
			pLoad->Commit();	// commit rows every 250 companies (20 rows/company)
		}
		
	} while (bRet);
	
	pLoad->FinishLoad();	//commit
	
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// HOLDING, HOLDING_HISTORY, TRADE, TRADE_HISTORY, SETTLEMENT, CASH_TRANSACTION, BROKER
void CGenerateAndLoad::GenerateAndLoadHoldingAndTrade()
{
	bool								bRet;
	CTradeGen*							pTradeGen;	

	CBaseLoader<HOLDING_ROW>*			pHoldingsLoad;
	CBaseLoader<HOLDING_HISTORY_ROW>*	pHoldingHistoryLoad;
	CBaseLoader<HOLDING_SUMMARY_ROW>*	pHoldingSummaryLoad;
	CBaseLoader<TRADE_ROW>*				pTradesLoad;
	// Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
	//CBaseLoader<TRADE_REQUEST_ROW>*		pRequestsLoad;
	CBaseLoader<SETTLEMENT_ROW>*		pSettlementLoad;
	CBaseLoader<TRADE_HISTORY_ROW>*		pHistoryLoad;
	CBaseLoader<CASH_TRANSACTION_ROW>*	pCashLoad;
	CBaseLoader<BROKER_ROW>*			pBrokerLoad;
	int									iCnt=0;
	int									i;
	int									iCurrentLoadUnit = 1;
	char								szCurrentLoadUnit[11];

	pHoldingsLoad = m_pLoaderFactory->CreateHoldingLoader();
	pHoldingHistoryLoad = m_pLoaderFactory->CreateHoldingHistoryLoader();
	pHoldingSummaryLoad = m_pLoaderFactory->CreateHoldingSummaryLoader();
	pTradesLoad = m_pLoaderFactory->CreateTradeLoader();
	// Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
	//pRequestsLoad = m_pLoaderFactory->CreateTradeRequestLoader();
	pSettlementLoad = m_pLoaderFactory->CreateSettlementLoader();
	pHistoryLoad = m_pLoaderFactory->CreateTradeHistoryLoader();
	pCashLoad = m_pLoaderFactory->CreateCashTransactionLoader();
	pBrokerLoad = m_pLoaderFactory->CreateBrokerLoader();

	m_pOutput->OutputStart("Generating TRADE, SETTLEMENT, TRADE HISTORY, CASH TRANSACTION, "
							"HOLDING_HISTORY, HOLDING_SUMMARY, HOLDING, and BROKER tables...");

	pTradeGen = new CTradeGen(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer, 
							  m_iTotalCustomers, m_iLoadUnitSize,
							  m_iScaleFactor, m_iHoursOfInitialTrades);

	// Generate and load one load unit at a time.
	//
	do
	{
		pTradesLoad->Init();			
		pSettlementLoad->Init();		
		pHistoryLoad->Init();			
		pCashLoad->Init();				
		pBrokerLoad->Init();			
		pHoldingHistoryLoad->Init();
		pHoldingsLoad->Init();
		pHoldingSummaryLoad->Init();
		// Not loading TRADE_REQUEST table
		//pRequestsLoad->Init();

		// Generate and load trades for this load unit.
		//
		do
		{	
			bRet = pTradeGen->GenerateNextTrade();

			pTradesLoad->WriteNextRecord(pTradeGen->GetTradeRow());

			for ( i=0; i<pTradeGen->GetTradeHistoryRowCount(); ++i)
			{
				pHistoryLoad->WriteNextRecord(pTradeGen->GetTradeHistoryRow(i));
			}

			if ( pTradeGen->GetSettlementRowCount() )
			{
				pSettlementLoad->WriteNextRecord(pTradeGen->GetSettlementRow());
			}

			if ( pTradeGen->GetCashTransactionRowCount() )
			{
				pCashLoad->WriteNextRecord(pTradeGen->GetCashTransactionRow());
			}

			for ( i=0; i<pTradeGen->GetHoldingHistoryRowCount(); ++i)
			{
				pHoldingHistoryLoad->WriteNextRecord(pTradeGen->GetHoldingHistoryRow(i));
			}

			/*if ((pTradeGen->GetTradeRow())->m_iTradeStatus == eCompleted)	// Not loading TRADE_REQUEST table
			{
			pRequestsLoad->WriteNextRecord(pTradeGen->GetTradeRequestRow());
			}*/

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pTradesLoad->Commit();			//commit		
				pSettlementLoad->Commit();		//commit
				pHistoryLoad->Commit();			//commit
				pCashLoad->Commit();
				pHoldingHistoryLoad->Commit();	//commit
				// Not loading TRADE_REQUEST table
				//pRequestsLoad->Commit();		//commit
			}

		} while (bRet);		

		// After trades generate and load BROKER table.
		//
		do
		{
			bRet = pTradeGen->GenerateNextBrokerRecord();

			pBrokerLoad->WriteNextRecord(pTradeGen->GetBrokerRow());

			// Commit rows every so often
			if (++iCnt % 10000 == 0)
			{
				pBrokerLoad->Commit();		//commit						
			}
		} while (bRet);

		m_pOutput->OutputProgress("t");

		//	Now generate and load HOLDING_SUMMARY rows for this load unit.
		//
		do 
		{
			bRet = pTradeGen->GenerateNextHoldingSummaryRow();

			pHoldingSummaryLoad->WriteNextRecord(pTradeGen->GetHoldingSummaryRow());

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pHoldingSummaryLoad->Commit();		//commit						
			}
		} while (bRet);		

		//	Now generate and load holdings for this load unit.
		//
		do 
		{
			bRet = pTradeGen->GenerateNextHolding();

			pHoldingsLoad->WriteNextRecord(pTradeGen->GetHoldingRow());

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pHoldingsLoad->Commit();		//commit						
			}
		} while (bRet);		

		pTradesLoad->FinishLoad();			//commit	
		pSettlementLoad->FinishLoad();		//commit
		pHistoryLoad->FinishLoad();			//commit
		pCashLoad->FinishLoad();			//commit
		pBrokerLoad->FinishLoad();			//commit
		pHoldingHistoryLoad->FinishLoad();	//commit
		pHoldingsLoad->FinishLoad();		//commit
		pHoldingSummaryLoad->FinishLoad();	//commit
		// Not loading TRADE_REQUEST table
		//pRequestsLoad->FinishLoad();		//commit

		// Output unit number for information
		sprintf(szCurrentLoadUnit, "%d", iCurrentLoadUnit++);

		m_pOutput->OutputProgress(szCurrentLoadUnit);

	} while (pTradeGen->InitNextLoadUnit());		

	delete pHoldingsLoad;
	delete pHoldingHistoryLoad;
	delete pHoldingSummaryLoad;
	delete pTradesLoad;
	// Not loading TRADE_REQUEST table
	//delete pRequestsLoad;
	delete pSettlementLoad;
	delete pHistoryLoad;
	delete pCashLoad;
	delete pBrokerLoad;

	delete pTradeGen;

	m_pOutput->OutputComplete(".loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// INDUSTRY
void CGenerateAndLoad::GenerateAndLoadIndustry()
{
	bool						bEndOfFile;
	CIndustryTable				Table( m_szInDir );
	CBaseLoader<INDUSTRY_ROW>*	pLoad = m_pLoaderFactory->CreateIndustryLoader();

	m_pOutput->OutputStart("Generating INDUSTRY table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// LAST_TRADE
void CGenerateAndLoad::GenerateAndLoadLastTrade()
{
	bool	bRet;
	CLastTradeTable					Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer
										  , m_iHoursOfInitialTrades);
	CBaseLoader<LAST_TRADE_ROW>*	pLoad = m_pLoaderFactory->CreateLastTradeLoader();	

	m_pOutput->OutputStart("Generating LAST TRADE table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();		

		pLoad->WriteNextRecord(Table.GetRow());
		
	} while (bRet);	

	pLoad->FinishLoad();	//commit
		
	delete pLoad;
	
	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// NEWS_ITEM, NEWS_XREF
void CGenerateAndLoad::GenerateAndLoadNewsItemAndNewsXRef()
{
	bool							bRet;
	// allocated on the heap because contains 100KB item
	CNewsItemAndXRefTable*			pTable = new CNewsItemAndXRefTable(	m_inputFiles, 
																		m_iCustomerCount, 
																		m_iStartFromCustomer);
	CBaseLoader<NEWS_ITEM_ROW>*		pNewsItemLoad = m_pLoaderFactory->CreateNewsItemLoader();
	CBaseLoader<NEWS_XREF_ROW>*		pNewsXRefLoad = m_pLoaderFactory->CreateNewsXRefLoader();
	INT64							iCnt=0;

	m_pOutput->OutputStart("Generating NEWS_ITEM and NEWS_XREF table...");

	pNewsItemLoad->Init();
	pNewsXRefLoad->Init();

	do
	{
		bRet = pTable->GenerateNextRecord();

		pNewsItemLoad->WriteNextRecord(pTable->GetNewsItemRow());
		
		pNewsXRefLoad->WriteNextRecord(pTable->GetNewsXRefRow());
		
		if (++iCnt % 1000 == 0)	// output progress every 1000 rows because each row generation takes a lot of time
		{
			m_pOutput->OutputProgress(".");	//output progress

			pNewsItemLoad->Commit();
			pNewsXRefLoad->Commit();
		}

	} while (bRet);
	pNewsItemLoad->FinishLoad();	//commit
	pNewsXRefLoad->FinishLoad();	//commit
	delete pNewsItemLoad;
	delete pNewsXRefLoad;
	delete pTable;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// SECTOR
void CGenerateAndLoad::GenerateAndLoadSector()
{
	bool						bEndOfFile;
	CSectorTable				Table( m_szInDir );
	CBaseLoader<SECTOR_ROW>*	pLoad = m_pLoaderFactory->CreateSectorLoader();

	m_pOutput->OutputStart("Generating SECTOR table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// SECURITY
void CGenerateAndLoad::GenerateAndLoadSecurity()
{
	bool							bRet;
	CSecurityTable					Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<SECURITY_ROW>*		pLoad = m_pLoaderFactory->CreateSecurityLoader();
	INT64							iCnt=0;	

	m_pOutput->OutputStart("Generating SECURITY table...");

	pLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();

		pLoad->WriteNextRecord(Table.GetRow());
		
		if (++iCnt % 20000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress
		}

	} while (bRet);

	pLoad->FinishLoad();	//commit	

	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// STATUS_TYPE
void CGenerateAndLoad::GenerateAndLoadStatusType()
{
	bool							bEndOfFile;
	CStatusTypeTable				Table( m_szInDir );
	CBaseLoader<STATUS_TYPE_ROW>*	pLoad = m_pLoaderFactory->CreateStatusTypeLoader();

	m_pOutput->OutputStart("Generating STATUS_TYPE table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// TAXRATE
void CGenerateAndLoad::GenerateAndLoadTaxrate()
{
	bool						bMoreRecords;
	CTaxrateTable				Table( m_inputFiles );
	CBaseLoader<TAXRATE_ROW>*	pLoad = m_pLoaderFactory->CreateTaxrateLoader();

	m_pOutput->OutputStart("Generating TAXRATE table...");

	pLoad->Init();

	do
	{		
		bMoreRecords = Table.GenerateNextRecord();
		pLoad->WriteNextRecord(Table.GetRow());

	} while( bMoreRecords );

	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// TRADE_TYPE
void CGenerateAndLoad::GenerateAndLoadTradeType()
{
	bool							bEndOfFile;
	CTradeTypeTable					Table( m_szInDir );
	CBaseLoader<TRADE_TYPE_ROW>*	pLoad = m_pLoaderFactory->CreateTradeTypeLoader();

	m_pOutput->OutputStart("Generating TRADE_TYPE table...");

	pLoad->Init();

	bEndOfFile = Table.GenerateNextRecord();
	while( !bEndOfFile )
	{
		pLoad->WriteNextRecord(Table.GetRow());
		bEndOfFile = Table.GenerateNextRecord();
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// WATCH_LIST, WATCH_ITEM
void CGenerateAndLoad::GenerateAndLoadWatchListAndWatchItem()
{
	bool							bRet;
	CWatchListsAndItemsTable		Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
	CBaseLoader<WATCH_LIST_ROW>*	pWatchListsLoad = m_pLoaderFactory->CreateWatchListLoader();
	CBaseLoader<WATCH_ITEM_ROW>*	pWatchItemsLoad = m_pLoaderFactory->CreateWatchItemLoader();
	INT64							iCnt=0;
	int								i;

	m_pOutput->OutputStart("Generating WATCH_LIST table and WATCH_ITEM table...");

	pWatchListsLoad->Init();
	pWatchItemsLoad->Init();

	do
	{
		bRet = Table.GenerateNextRecord();
		
		pWatchListsLoad->WriteNextRecord(Table.GetWLRow());

		for (i=0; i<Table.GetWICount(); ++i)
		{
			pWatchItemsLoad->WriteNextRecord(Table.GetWIRow(i));

			if (++iCnt % 20000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress

				pWatchListsLoad->Commit();	//commit
				pWatchItemsLoad->Commit();	//commit
			}			
		}
	} while (bRet);

	pWatchListsLoad->FinishLoad();	//commit
	pWatchItemsLoad->FinishLoad();	//commit

	delete pWatchListsLoad;
	delete pWatchItemsLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// ZIP_CODE
void CGenerateAndLoad::GenerateAndLoadZipCode()
{
	CZipCodeTable				Table( m_inputFiles );
	CBaseLoader<ZIP_CODE_ROW>*	pLoad = m_pLoaderFactory->CreateZipCodeLoader();

	m_pOutput->OutputStart("Generating ZIP_CODE table...");

	pLoad->Init();

	while( Table.GenerateNextRecord() )
	{
		pLoad->WriteNextRecord(Table.GetRow());
	}
	pLoad->FinishLoad();	//commit
	delete pLoad;

	m_pOutput->OutputComplete("loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}

// All tables that are constant in size
//
// Spec definition: Fixed tables.
//
void CGenerateAndLoad::GenerateAndLoadFixedTables()
{
	// Tables from flat files first
	GenerateAndLoadCharge();
	GenerateAndLoadCommissionRate();
	GenerateAndLoadExchange();
	GenerateAndLoadIndustry();
	GenerateAndLoadSector();
	GenerateAndLoadStatusType();
	GenerateAndLoadTaxrate();
	GenerateAndLoadTradeType();
	GenerateAndLoadZipCode();	
}

// All tables (except BROKER) that scale with the size of 
// the CUSTOMER table, but do not grow in runtime. 
//
// Spec definition: Scaling tables.
//
void CGenerateAndLoad::GenerateAndLoadScalingTables()
{
	// Customer-related tables
	//
	GenerateAndLoadAddress();	
	GenerateAndLoadCustomer();
	GenerateAndLoadCustomerAccountAndAccountPermission();
	GenerateAndLoadCustomerTaxrate();
	GenerateAndLoadWatchListAndWatchItem();

	// Now security/company related tables
	//
	GenerateAndLoadCompany();
	GenerateAndLoadCompanyCompetitor();
	GenerateAndLoadDailyMarket();
	GenerateAndLoadFinancial();
	GenerateAndLoadLastTrade();
	GenerateAndLoadNewsItemAndNewsXRef();
	GenerateAndLoadSecurity();
}

// All trade related tables and BROKER (included here to
// facilitate generation of a consistent database).
//
// Spec definition: Growing tables.
//
void CGenerateAndLoad::GenerateAndLoadGrowingTables()
{
	GenerateAndLoadHoldingAndTrade();
}
