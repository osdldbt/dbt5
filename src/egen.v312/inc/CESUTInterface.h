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
*						specific class for commmunicating with the SUT for all
*						customer-initiated transactions.
*
******************************************************************************/

#ifndef CE_SUT_INTERFACE_H
#define CE_SUT_INTERFACE_H

#include "TxnHarnessStructs.h"

namespace TPCE
{

class CCESUTInterface
{
public:
	virtual bool BrokerVolume( PBrokerVolumeTxnInput pTxnInput ) = 0;												// return whether it was successful
	virtual bool CustomerPosition( PCustomerPositionTxnInput pTxnInput ) = 0;										// return whether it was successful
	virtual bool MarketWatch( PMarketWatchTxnInput pTxnInput ) = 0;													// return whether it was successful
	virtual bool SecurityDetail( PSecurityDetailTxnInput pTxnInput ) = 0;											// return whether it was successful
	virtual bool TradeLookup( PTradeLookupTxnInput pTxnInput ) = 0;													// return whether it was successful
	virtual bool TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner ) = 0;	// return whether it was successful
	virtual bool TradeStatus( PTradeStatusTxnInput pTxnInput ) = 0;													// return whether it was successful
	virtual bool TradeUpdate( PTradeUpdateTxnInput pTxnInput ) = 0;													// return whether it was successful
};

}	// namespace TPCE

#endif //CE_SUT_INTERFACE_H
