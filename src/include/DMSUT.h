/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * DM - SUT Interface class
 * 12 August 2006
 */

#ifndef DM_SUT_H
#define DM_SUT_H

#include "DM.h"
#include "locking.h"

#include "BaseInterface.h"
using namespace TPCE;

class CDMSUT : public CDMSUTInterface, public CBaseInterface
{
public:
	CDMSUT(char *, const int, ofstream *, ofstream *, CMutex *, CMutex *);
	~CDMSUT(void);

	// return whether it was successful
	virtual bool DataMaintenance(PDataMaintenanceTxnInput pTxnInput);
	// return whether it was successful
	virtual bool TradeCleanup(PTradeCleanupTxnInput pTxnInput);
};

#endif // DM_SUT_H
