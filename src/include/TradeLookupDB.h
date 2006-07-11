/*
 * TradeLookupDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 08 July 2006
 */

#ifndef TRADE_LOOKUP_DB_H
#define TRADE_LOOKUP_DB_H
 
namespace TPCE
{

class CTradeLookupDB : public CTxnBaseDB
{
	TTradeLookupFrame1Input		Frame1Input;
	TTradeLookupFrame1Output	Frame1Output;
	TTradeLookupFrame2Input		Frame2Input;
	TTradeLookupFrame2Output	Frame2Output;
	TTradeLookupFrame3Input		Frame3Input;
	TTradeLookupFrame3Output	Frame3Output;
	TTradeLookupFrame4Input		Frame4Input;
	TTradeLookupFrame4Output	Frame4Output;

public:
	CTradeLookupDB(CDBConnection *pDBConn);
	~CTradeLookupDB();

	void DoTradeLookupFrame1(PTradeLookupFrame1Input pFrame1Input, PTradeLookupFrame1Output pFrame1Output);
	void DoTradeLookupFrame2(PTradeLookupFrame2Input pFrame2Input, PTradeLookupFrame2Output pFrame2Output);
	void DoTradeLookupFrame3(PTradeLookupFrame3Input pFrame3Input, PTradeLookupFrame3Output pFrame3Output);
	void DoTradeLookupFrame4(PTradeLookupFrame4Input pFrame4Input, PTradeLookupFrame4Output pFrame4Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// TRADE_LOOKUP_DB_H
