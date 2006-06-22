/*
 * TradeStatusDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */

#ifndef TRADE_STATUS_DB_H
#define TRADE_STATUS_DB_H

//#include "transactions.h"
 
namespace TPCE
{

class CTradeStatusDB : public CTxnBaseDB
{
	TTradeStatusFrame1Input		Frame1Input;
	TTradeStatusFrame1Output	Frame1Output;

public:
	CTradeStatusDB(CDBConnection *pDBConn);
	~CTradeStatusDB();

	void DoTradeStatusFrame1(PTradeStatusFrame1Input pFrame1Input, PTradeStatusFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_STATUS_DB_H
