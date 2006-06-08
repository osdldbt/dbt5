/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Base interface for a class that implements Send To Market.
*	Implementations should subclass from this interface to include platform-dependent
*	functionality.
*/
#ifndef TXN_HARNESS_SEND_TO_MARKET_INTERFACE_H
#define TXN_HARNESS_SEND_TO_MARKET_INTERFACE_H

namespace TPCE
{

class CSendToMarketInterface
{
public:
	virtual bool SendToMarket(TTradeRequest &trade_mes) = 0;//returns whether the Send To Market was successful
};

}	// namespace TPCE

#endif //TXN_HARNESS_SEND_TO_MARKET_INTERFACE_H
