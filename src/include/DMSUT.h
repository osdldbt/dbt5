/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * DM - SUT Interface class
 * 12 August 2006
 */

#ifndef DM_SUT_H
#define DM_SUT_H

#include "DMSUTInterface.h"
#include "locking.h"

class CDMSUT : public CDMSUTInterface, public CBaseInterface
{

public:

	CDMSUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix,
			CMutex* pLogLock, CMutex* pMixLock);
	~CDMSUT(void);

	// return whether it was successful
	virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput );
	// return whether it was successful
	virtual bool TradeCleanup( PTradeCleanupTxnInput pTxnInput );
};

#endif	// DM_SUT_H
