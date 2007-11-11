/*
 * DataMaintenanceDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 17 July 2006
 */

#ifndef DATA_MAINTENANCE_DB_H
#define DATA_MAINTENANCE_DB_H

#include <TxnHarnessDBInterface.h> 
 
namespace TPCE
{

class CDataMaintenanceDB : public CTxnBaseDB, public CDataMaintenanceDBInterface
{
public:
	CDataMaintenanceDB(CDBConnection *pDBConn);
	~CDataMaintenanceDB();

	void DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn,
			TDataMaintenanceFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// DATA_MAINTENANCE_DB_H
