/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Interface base class to be used for deriving a sponsor 
*						specific class for commmunicating with the SUT. The 
*						recommended implementation for each of method is to
*						perform a memcpy of the provided data structure and
*						then return SUCCESS. A seperate thread of execution
*						can then pick up the copy of the data, and perform the
*						actual transaction against the SUT. This asynchronicity
*						prevents the MEE from getting held up doing the actual
*						Trade-Result or Market-Feed.
*
******************************************************************************/

#ifndef MEE_SUT_INTERFACE_H
#define MEE_SUT_INTERFACE_H

#include "TxnHarnessStructs.h"

namespace TPCE
{

class CMEESUTInterface
{
public:
	virtual bool TradeResult( PTradeResultTxnInput pTxnInput ) = 0;	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput ) = 0;	// return whether it was successful
};

}	// namespace TPCE

#endif //MEE_SUT_INTERFACE_H
