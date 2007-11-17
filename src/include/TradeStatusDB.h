/*
 * TradeStatusDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 June 2006
 */

#ifndef TRADE_STATUS_DB_H
#define TRADE_STATUS_DB_H

#include <TxnHarnessDBInterface.h>
 
namespace TPCE
{

class CTradeStatusDB : public CTxnBaseDB, public CTradeStatusDBInterface
{
public:
	CTradeStatusDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};

	~CTradeStatusDB() {};

	virtual void DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
			TTradeStatusFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

}	// namespace TPCE

#endif	// TRADE_STATUS_DB_H
