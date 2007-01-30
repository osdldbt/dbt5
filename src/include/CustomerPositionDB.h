/*
 * CustomerPositionDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 12 July 2006
 */

#ifndef CUSTOMER_POSITION_DB_H
#define CUSTOMER_POSITION_DB_H
 
namespace TPCE
{

class CCustomerPositionDB : public CTxnBaseDB
{
	TCustomerPositionFrame1Input	Frame1Input;
	TCustomerPositionFrame1Output	Frame1Output;
	TCustomerPositionFrame2Input	Frame2Input;
	TCustomerPositionFrame2Output	Frame2Output;
	TCustomerPositionFrame3Output	Frame3Output;

public:
	CCustomerPositionDB(CDBConnection *pDBConn);
	~CCustomerPositionDB();

	void DoCustomerPositionFrame1(PCustomerPositionFrame1Input pFrame1Input,
			PCustomerPositionFrame1Output pFrame1Output);
	void DoCustomerPositionFrame2(PCustomerPositionFrame2Input pFrame2Input,
			PCustomerPositionFrame2Output pFrame2Output);
	void DoCustomerPositionFrame3(PCustomerPositionFrame3Output pFrame3Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// CUSTOMER_POSITION_DB_H
