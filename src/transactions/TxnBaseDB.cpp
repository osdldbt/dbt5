/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 June 2006
 */

#include "TxnBaseDB.h"

#include "TxnHarnessSendToMarketInterface.h"

CTxnBaseDB::CTxnBaseDB(CDBConnection *pDB)
{
	this->pDB = pDB;
}

CTxnBaseDB::~CTxnBaseDB()
{
}

void CTxnBaseDB::commitTransaction()
{
	pDB->commit();
}

string CTxnBaseDB::escape(string s)
{
	return pDB->escape(s);
}

void CTxnBaseDB::execute(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TDataMaintenanceFrame1Input *pIn,
		TDataMaintenanceFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TMarketFeedFrame1Input *pIn,
		TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pMarketExchange)
{
	pDB->execute(pIn, pOut, pMarketExchange);
}

void CTxnBaseDB::execute(const TMarketWatchFrame1Input *pIn,
		TMarketWatchFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeCleanupFrame1Input *pIn,
		TTradeCleanupFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeLookupFrame1Input *pIn,
		TTradeLookupFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeLookupFrame2Input *pIn,
		TTradeLookupFrame2Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeLookupFrame3Input *pIn,
		TTradeLookupFrame3Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeLookupFrame4Input *pIn,
		TTradeLookupFrame4Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame5Input *pIn,
		TTradeResultFrame5Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeUpdateFrame2Input *pIn,
		TTradeUpdateFrame2Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::execute(const TTradeUpdateFrame3Input *pIn,
		TTradeUpdateFrame3Output *pOut)
{
	pDB->execute(pIn, pOut);
}

void CTxnBaseDB::rollbackTransaction()
{
	pDB->rollback();
}

void CTxnBaseDB::startTransaction()
{
	pDB->begin();
}

void CTxnBaseDB::setReadCommitted()
{
	pDB->setReadCommitted();
}

void CTxnBaseDB::setReadUncommitted()
{
	pDB->setReadUncommitted();
}

void CTxnBaseDB::setRepeatableRead()
{
	pDB->setRepeatableRead();
}

void CTxnBaseDB::setSerializable()
{
	pDB->setSerializable();
}
