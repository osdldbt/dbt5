/*
 * CustomerPositionDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 12 July 2006
 */

#ifndef CUSTOMER_POSITION_DB_H
#define CUSTOMER_POSITION_DB_H

#include <TxnHarnessDBInterface.h> 
 
namespace TPCE
{

class CCustomerPositionDB : public CTxnBaseDB,
		public CCustomerPositionDBInterface
{
public:
	CCustomerPositionDB(CDBConnection *pDBConn);
	~CCustomerPositionDB();

	virtual void DoCustomerPositionFrame1(
			const TCustomerPositionFrame1Input *pIn,
			TCustomerPositionFrame1Output *pOut);
	virtual void DoCustomerPositionFrame2(
			const TCustomerPositionFrame2Input *pIn,
			TCustomerPositionFrame2Output *pOut);
	virtual void DoCustomerPositionFrame3(TCustomerPositionFrame3Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);
};

}	// namespace TPCE

#endif	// CUSTOMER_POSITION_DB_H
