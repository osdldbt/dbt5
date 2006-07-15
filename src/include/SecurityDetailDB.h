/*
 * SecurityDetailDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 13 July 2006
 */

#ifndef SECURITY_DETAIL_DB_H
#define SECURITY_DETAIL_DB_H
 
namespace TPCE
{

class CSecurityDetailDB : public CTxnBaseDB
{
	TSecurityDetailFrame1Input	Frame1Input;
	TSecurityDetailFrame1Output	Frame1Output;

public:
	CSecurityDetailDB(CDBConnection *pDBConn);
	~CSecurityDetailDB();

	void DoSecurityDetailFrame1(PSecurityDetailFrame1Input pFrame1Input, PSecurityDetailFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// SECURITY_DETAIL_DB_H
