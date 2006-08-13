/*
 * DMSUT.h
 * DM - SUT Interface class
 *
 * 2006 Rilson Nascimento
 *
 * 17 July 2006
 */

#ifndef DM_SUT_TEST_H
#define DM_SUT_TEST_H

#include "DMSUTInterface.h"

namespace TPCE
{

class CDMSUTtest : public CDMSUTInterface
{
protected:
	CDBConnection*		m_pDBConnection;

public:
	CDMSUTtest(CDBConnection *pDBConn);
	~CDMSUTtest();

	virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput );	// return whether it was successful
	virtual bool TradeCleanup( PTradeCleanupTxnInput pTxnInput );	// return whether it was successful
};

}	// namespace TPCE

#endif	// DM_SUT_TEST_H
