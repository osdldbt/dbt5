/*
 * DataMaintenanceDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 17 July 2006
 */

#ifndef DATA_MAINTENANCE_DB_H
#define DATA_MAINTENANCE_DB_H
 
namespace TPCE
{

class CDataMaintenanceDB : public CTxnBaseDB
{
	TDataMaintenanceFrame1Input	Frame1Input;
	TDataMaintenanceFrame1Output	Frame1Output;

public:
	CDataMaintenanceDB(CDBConnection *pDBConn);
	~CDataMaintenanceDB();

	void DoDataMaintenanceFrame1(PDataMaintenanceFrame1Input pFrame1Input,
			PDataMaintenanceFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// DATA_MAINTENANCE_DB_H
