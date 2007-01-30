/*
 * DMSUTtest.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 17 July 2006
 */

#include <transactions.h>
#include <DMSUTtest.h>

using namespace TPCE;

CDMSUTtest::CDMSUTtest(CDBConnection *pDBConn)
: m_pDBConnection(pDBConn)
{
}

CDMSUTtest::~CDMSUTtest()
{
}

bool CDMSUTtest::DataMaintenance( PDataMaintenanceTxnInput pTxnInput )
{
	// Data Maintenance harness code (TPC provided)
	// this class uses our implementation of CDataMaintenanceDB class
	CDataMaintenance		m_DataMaintenance(m_pDBConnection);

	// Data Maintenance output parameters
	TDataMaintenanceTxnOutput	m_DataMaintenanceTxnOutput;
	
	// Perform Data Maintenance
	m_DataMaintenance.DoTxn( pTxnInput, &m_DataMaintenanceTxnOutput);
}

bool CDMSUTtest::TradeCleanup( PTradeCleanupTxnInput pTxnInput )
{
	// Data Maintenance harness code (TPC provided)
	// this class uses our implementation of CTradeCleanupDB class
	CTradeCleanup		m_TradeCleanup(m_pDBConnection);

	// Data Maintenance output parameters
	TTradeCleanupTxnOutput	m_TradeCleanupTxnOutput;
	
	// Perform Trade Cleanup	
	m_TradeCleanup.DoTxn( pTxnInput, &m_TradeCleanupTxnOutput);
}
