/*
 * TradeCleanupDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 18 July 2006
 */

#ifndef TRADE_CLEANUP_DB_H
#define TRADE_CLEANUP_DB_H
 
namespace TPCE
{

class CTradeCleanupDB : public CTxnBaseDB
{
	TTradeCleanupFrame1Input	Frame1Input;
	TTradeCleanupFrame1Output	Frame1Output;

public:
	CTradeCleanupDB(CDBConnection *pDBConn);
	~CTradeCleanupDB();

	void DoTradeCleanupFrame1(PTradeCleanupFrame1Input pFrame1Input,
			PTradeCleanupFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_CLEANUP_DB_H
