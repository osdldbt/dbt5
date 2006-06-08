/******************************************************************************
*	(c) Copyright 2005, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Interface base class to be used for deriving a sponsor 
*						specific class for commmunicating with the SUT for 
*						the Data-Maintenance transaction.
*
******************************************************************************/

#ifndef DM_SUT_INTERFACE_H
#define DM_SUT_INTERFACE_H

#include "TxnHarnessStructs.h"

namespace TPCE
{

class CDMSUTInterface
{
public:
	virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput ) = 0;	// return whether it was successful
};

}	// namespace TPCE

#endif //DM_SUT_INTERFACE_H
