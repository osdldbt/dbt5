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
	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = DATA_MAINTENANCE;
	memcpy(&(pRequest->TxnInput.DataMaintenanceTxnInput), pTxnInput,
			sizeof(TDataMaintenanceTxnInput));

	return talkToSUT(pRequest);
}

// Trade Cleanup
bool
CDMSUT::TradeCleanup(PTradeCleanupTxnInput pTxnInput)
{
	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = TRADE_CLEANUP;
	memcpy(&(pRequest->TxnInput.TradeCleanupTxnInput), pTxnInput,
			sizeof(TTradeCleanupTxnInput));

	return talkToSUT(pRequest);
}
