/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_DATA_MAINTENANCE_H
#define TXN_HARNESS_DATA_MAINTENANCE_H

namespace TPCE
{

class CDataMaintenance
{
	CDataMaintenanceDB	m_db;		

public:
	CDataMaintenance(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	};

	void DoTxn( PDataMaintenanceTxnInput pTxnInput, PDataMaintenanceTxnOutput pTxnOutput )
	{
		m_db.DoDataMaintenanceFrame1( pTxnInput, pTxnOutput);
	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException)
	{
		m_db.Cleanup(pException);
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_DATA_MAINTENANCE_H
