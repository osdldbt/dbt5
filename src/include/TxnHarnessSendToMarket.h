/*
 * TxnHarnessSendToMarket.h
 * (Dummy) SendToMarket class
 *
 * 2006 Rilson Nascimento
 *
 * 06 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_H
#define TXN_HARNESS_SENDTOMARKET_H

#include "transactions.h"
#include "TxnHarnessSendToMarketInterface.h"

namespace TPCE
{

class CSendToMarket : public CSendToMarketInterface
{
public:
	CSendToMarket();
	~CSendToMarket();

	virtual bool SendToMarket(TTradeRequest &trade_mes);
};

}	// namespace TPCE

#endif	// TXN_HARNESS_SENDTOMARKET_H
