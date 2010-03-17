/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 13 July 2006
 */

#ifndef BROKER_VOLUME_DB_H
#define BROKER_VOLUME_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
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
