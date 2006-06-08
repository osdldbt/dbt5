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
*	Description:		These are the different actions the MEE can take 
*						with an inbound trade request.
*
******************************************************************************/

#ifndef MEE_TRADE_REQUEST_ACTIONS_H
#define MEE_TRADE_REQUEST_ACTIONS_H

namespace TPCE
{

enum eMEETradeRequestAction 
{
	eMEEProcessOrder = 0,
	eMEESetLimitOrderTrigger
};

} //namespace TPCE

#endif //MEE_TRADE_REQUEST_ACTIONS_H
