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

void CTxnBaseDB::execute(string sql, TBrokerVolumeFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TCustomerPositionFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TCustomerPositionFrame2Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TDataMaintenanceFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pMarketExchange)
{
	pDB->execute(sql, pOut, pMarketExchange);
}

void CTxnBaseDB::execute(string sql, TMarketWatchFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TSecurityDetailFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeCleanupFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeLookupFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeLookupFrame2Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeLookupFrame3Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeLookupFrame4Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeOrderFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeOrderFrame2Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeOrderFrame3Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeOrderFrame4Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame2Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame3Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame4Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame5Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeResultFrame6Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeStatusFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeUpdateFrame1Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeUpdateFrame2Output *pOut)
{
	pDB->execute(sql, pOut);
}

void CTxnBaseDB::execute(string sql, TTradeUpdateFrame3Output *pOut)
{
	pDB->execute(sql, pOut);
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
