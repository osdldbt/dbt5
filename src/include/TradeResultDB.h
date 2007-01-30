/*
 * TradeResultDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 07 July 2006
 */

#ifndef TRADE_RESULT_DB_H
#define TRADE_RESULT_DB_H
 
namespace TPCE
{

class CTradeResultDB : public CTxnBaseDB
{
	TTradeResultFrame1Input		Frame1Input;
	TTradeResultFrame1Output	Frame1Output;
	TTradeResultFrame2Input		Frame2Input;
	TTradeResultFrame2Output	Frame2Output;
	TTradeResultFrame3Input		Frame3Input;
	TTradeResultFrame3Output	Frame3Output;
	TTradeResultFrame4Input		Frame4Input;
	TTradeResultFrame4Output	Frame4Output;
	TTradeResultFrame5Input		Frame5Input;
	TTradeResultFrame5Output	Frame5Output;
	TTradeResultFrame6Input		Frame6Input;
	TTradeResultFrame6Output	Frame6Output;

public:
	CTradeResultDB(CDBConnection *pDBConn);
	~CTradeResultDB();

	void DoTradeResultFrame1(PTradeResultFrame1Input pFrame1Input,
			PTradeResultFrame1Output pFrame1Output);
	void DoTradeResultFrame2(PTradeResultFrame2Input pFrame2Input,
			PTradeResultFrame2Output pFrame2Output);
	void DoTradeResultFrame3(PTradeResultFrame3Input pFrame3Input,
			PTradeResultFrame3Output pFrame3Output);
	void DoTradeResultFrame4(PTradeResultFrame4Input pFrame4Input,
			PTradeResultFrame4Output pFrame4Output);
	void DoTradeResultFrame5(PTradeResultFrame5Input pFrame5Input,
			PTradeResultFrame5Output pFrame5Output);
	void DoTradeResultFrame6(PTradeResultFrame6Input pFrame6Input,
			PTradeResultFrame6Output pFrame6Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_RESULT_DB_H
