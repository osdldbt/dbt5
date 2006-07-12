/*
 * TradeUpdateDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 11 July 2006
 */

#ifndef TRADE_UPDATE_DB_H
#define TRADE_UPDATE_DB_H
 
namespace TPCE
{

class CTradeUpdateDB : public CTxnBaseDB
{
	TTradeUpdateFrame1Input		Frame1Input;
	TTradeUpdateFrame1Output	Frame1Output;
	TTradeUpdateFrame2Input		Frame2Input;
	TTradeUpdateFrame2Output	Frame2Output;
	TTradeUpdateFrame3Input		Frame3Input;
	TTradeUpdateFrame3Output	Frame3Output;

public:
	CTradeUpdateDB(CDBConnection *pDBConn);
	~CTradeUpdateDB();

	void DoTradeUpdateFrame1(PTradeUpdateFrame1Input pFrame1Input, PTradeUpdateFrame1Output pFrame1Output);
	void DoTradeUpdateFrame2(PTradeUpdateFrame2Input pFrame2Input, PTradeUpdateFrame2Output pFrame2Output);
	void DoTradeUpdateFrame3(PTradeUpdateFrame3Input pFrame3Input, PTradeUpdateFrame3Output pFrame3Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_UPDATE_DB_H
