/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * DM - SUT Interface class
 * 17 July 2006
 */

#ifndef DM_SUT_TEST_H
#define DM_SUT_TEST_H

#include "DMSUTInterface.h"

class CDMSUTtest : public CDMSUTInterface
{
protected:
	CDBConnection*		m_pDBConnection;

public:
	CDMSUTtest(CDBConnection *pDBConn);
	~CDMSUTtest();

	// return whether it was successful
	virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput );
	// return whether it was successful
	virtual bool TradeCleanup( PTradeCleanupTxnInput pTxnInput );
};

#endif	// DM_SUT_TEST_H
