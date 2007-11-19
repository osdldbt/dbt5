/*
 * TradeOrderDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 03 July 2006
 */

#ifndef TRADE_ORDER_DB_H
#define TRADE_ORDER_DB_H

#include <TxnHarnessDBInterface.h> 
 
using namespace TPCE;

class CTradeOrderDB : public CTxnBaseDB, public CTradeOrderDBInterface
{
public:
	CTradeOrderDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeOrderDB() {};

	virtual void DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
			TTradeOrderFrame1Output *pOut);
	virtual void DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
			TTradeOrderFrame2Output *pOut);
	virtual void DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
			TTradeOrderFrame3Output *pOut);
	virtual void DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
			TTradeOrderFrame4Output *pOut);
	virtual void DoTradeOrderFrame5(TTradeOrderFrame5Output *pOut);
	virtual void DoTradeOrderFrame6(TTradeOrderFrame6Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_ORDER_DB_H
