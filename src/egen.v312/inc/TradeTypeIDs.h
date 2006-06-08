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
*	Description:		Trade Type IDs corresponding to the TradeType.txt flat 
*						file.
*						Note: The order of enumeration members must match the
*						order of rows in the TradeType.txt flat file.
*
******************************************************************************/

#ifndef TRADE_TYPE_IDS_H
#define TRADE_TYPE_IDS_H

namespace TPCE
{

enum eTradeTypeID 
{
	eMarketBuy = 0,
	eMarketSell,
	eStopLoss,
	eLimitSell,
	eLimitBuy,

	eMaxTradeTypeID	// should be the last - contains the number of items in the enumeration
};

} //namespace TPCE

#endif //TRADE_TYPE_IDS_H
