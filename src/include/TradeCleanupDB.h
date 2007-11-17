/*
 * TradeCleanupDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 18 July 2006
 */

#ifndef TRADE_CLEANUP_DB_H
#define TRADE_CLEANUP_DB_H

#include <TxnHarnessDBInterface.h> 
 
namespace TPCE
{

class CTradeCleanupDB : public CTxnBaseDB, public CTradeCleanupDBInterface
{
public:
	CTradeCleanupDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeCleanupDB() {};

	virtual void DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn,
			TTradeCleanupFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};

};

}	// namespace TPCE

#endif	// TRADE_CLEANUP_DB_H
