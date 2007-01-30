/*
 * BrokerVolumeDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 13 July 2006
 */

#ifndef BROKER_VOLUME_DB_H
#define BROKER_VOLUME_DB_H
 
namespace TPCE
{

class CBrokerVolumeDB : public CTxnBaseDB
{
	TBrokerVolumeFrame1Input	Frame1Input;
	TBrokerVolumeFrame1Output	Frame1Output;

public:
	CBrokerVolumeDB(CDBConnection *pDBConn);
	~CBrokerVolumeDB();

	void DoBrokerVolumeFrame1(PBrokerVolumeFrame1Input pFrame1Input,
			PBrokerVolumeFrame1Output pFrame1Output);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// BROKER_VOLUME_DB_H
