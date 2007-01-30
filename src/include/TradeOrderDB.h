/*
 * TradeOrderDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 03 July 2006
 */

#ifndef TRADE_ORDER_DB_H
#define TRADE_ORDER_DB_H
 
namespace TPCE
{

class CTradeOrderDB : public CTxnBaseDB
{
	TTradeOrderFrame1Input		Frame1Input;
	TTradeOrderFrame1Output		Frame1Output;
	TTradeOrderFrame2Input		Frame2Input;
	TTradeOrderFrame2Output		Frame2Output;
	TTradeOrderFrame3Input		Frame3Input;
	TTradeOrderFrame3Output		Frame3Output;
	TTradeOrderFrame4Input		Frame4Input;
	TTradeOrderFrame4Output		Frame4Output;
	TTradeOrderFrame5Output		Frame5Output;
	TTradeOrderFrame6Output		Frame6Output;

public:
	CTradeOrderDB(CDBConnection *pDBConn);
	~CTradeOrderDB();

	void DoTradeOrderFrame1(PTradeOrderFrame1Input pFrame1Input,
			PTradeOrderFrame1Output pFrame1Output);
	void DoTradeOrderFrame2(PTradeOrderFrame2Input pFrame2Input,
			PTradeOrderFrame2Output pFrame2Output);
	void DoTradeOrderFrame3(PTradeOrderFrame3Input pFrame3Input,
			PTradeOrderFrame3Output pFrame3Output);
	void DoTradeOrderFrame4(PTradeOrderFrame4Input pFrame4Input,
			PTradeOrderFrame4Output pFrame4Output);
	void DoTradeOrderFrame5(PTradeOrderFrame5Output pFrame5Output);
	void DoTradeOrderFrame6(PTradeOrderFrame6Output pFrame6Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_ORDER_DB_H
