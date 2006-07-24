/*
 * TxnHarnessSendToMarketTest.h
 * Test SendToMarket class. Used to call Market-Feed
 *
 * 2006 Rilson Nascimento
 *
 * 22 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_TEST_H
#define TXN_HARNESS_SENDTOMARKET_TEST_H

namespace TPCE
{

class CSendToMarketTest : public CSendToMarketInterface
{
public:
	CSendToMarketTest();
	~CSendToMarketTest();

	virtual bool SendToMarket(TTradeRequest &trade_mes);
};

}	// namespace TPCE

#endif	// TXN_HARNESS_SENDTOMARKET_TEST_H
