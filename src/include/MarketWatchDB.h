/*
 * MarketWatchDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 15 July 2006
 */

#ifndef MARKET_WATCH_DB_H
#define MARKET_WATCH_DB_H
 
namespace TPCE
{

class CMarketWatchDB : public CTxnBaseDB
{
	TMarketWatchFrame1Input		Frame1Input;
	TMarketWatchFrame1Output	Frame1Output;

public:
	CMarketWatchDB(CDBConnection *pDBConn);
	~CMarketWatchDB();

	void DoMarketWatchFrame1(PMarketWatchFrame1Input pFrame1Input,
			PMarketWatchFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// MARKET_WATCH_DB_H
