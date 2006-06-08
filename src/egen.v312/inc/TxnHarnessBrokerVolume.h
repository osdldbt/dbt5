/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_BROKER_VOLUME_H
#define TXN_HARNESS_BROKER_VOLUME_H

namespace TPCE
{

class CBrokerVolume
{
	CBrokerVolumeDB	m_db;		

public:
	CBrokerVolume(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	};

	void DoTxn( PBrokerVolumeTxnInput pTxnInput, PBrokerVolumeTxnOutput pTxnOutput )
	{
		TBrokerVolumeFrame1Output	Frame1Output;
		memset(&Frame1Output, 0, sizeof( Frame1Output ));

		m_db.DoBrokerVolumeFrame1( pTxnInput, &Frame1Output );

		pTxnOutput->list_len = Frame1Output.list_len;
		pTxnOutput->status = Frame1Output.status;
		for( INT32 ii=0; ii < Frame1Output.list_len; ii++ )
		{
			pTxnOutput->volume[ii] = Frame1Output.volume[ii];
		}
	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException)
	{
		m_db.Cleanup(pException);
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_BROKER_VOLUME_H
