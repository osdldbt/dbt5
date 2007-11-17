/*
 * TradeUpdateDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 11 July 2006
 */

#ifndef TRADE_UPDATE_DB_H
#define TRADE_UPDATE_DB_H

#include <TxnHarnessDBInterface.h> 
 
namespace TPCE
{

class CTradeUpdateDB : public CTxnBaseDB, public CTradeUpdateDBInterface
{
public:
	CTradeUpdateDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeUpdateDB() {};

	virtual void DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
			TTradeUpdateFrame1Output *pOut);
	virtual void DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
			TTradeUpdateFrame2Output *pOut);
	virtual void DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
			TTradeUpdateFrame3Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

}	// namespace TPCE

#endif	// TRADE_UPDATE_DB_H
