/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 12 August 2006
 */

#include "DMSUT.h"

// constructor
CDMSUT::CDMSUT(char *outputDirectory, char *addr, const int iListenPort)
: CBaseInterface("dm", outputDirectory, addr, iListenPort)
{
}

// destructor
CDMSUT::~CDMSUT() {}

// Data Maintenance
bool
CDMSUT::DataMaintenance(PDataMaintenanceTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = DATA_MAINTENANCE;
	memcpy(&(request.TxnInput.DataMaintenanceTxnInput), pTxnInput,
			sizeof(TDataMaintenanceTxnInput));

	return talkToSUT(&request);
}

// Trade Cleanup
bool
CDMSUT::TradeCleanup(PTradeCleanupTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_CLEANUP;
	memcpy(&(request.TxnInput.TradeCleanupTxnInput), pTxnInput,
			sizeof(TTradeCleanupTxnInput));

	return talkToSUT(&request);
}
