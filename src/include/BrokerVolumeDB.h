/*
 * BrokerVolumeDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 July 2006
 */

#ifndef BROKER_VOLUME_DB_H
#define BROKER_VOLUME_DB_H

#include <TxnHarnessDBInterface.h> 
 
using namespace TPCE;

class CBrokerVolumeDB : public CTxnBaseDB, public CBrokerVolumeDBInterface
{
public:
	CBrokerVolumeDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CBrokerVolumeDB() {};

	virtual void DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
			TBrokerVolumeFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// BROKER_VOLUME_DB_H
