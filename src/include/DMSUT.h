/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
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

class CDMSUT: public CDMSUTInterface, public CBaseInterface
{
public:
	CDMSUT(char *, char *, const int);
	~CDMSUT(void);

	bool DataMaintenance(PDataMaintenanceTxnInput);
	bool TradeCleanup(PTradeCleanupTxnInput);
};

#endif // DM_SUT_H
