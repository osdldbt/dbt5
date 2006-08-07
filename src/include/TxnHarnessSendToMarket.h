/*
 * TxnHarnessSendToMarket.h
 * SendToMarket class
 *
 * 2006 Rilson Nascimento
 *
 * 06 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_H
#define TXN_HARNESS_SENDTOMARKET_H

namespace TPCE
{

class CSendToMarket : public CSendToMarketInterface
{
	CSocket		m_Socket;
	int		m_MEport;

public:
	CSendToMarket(int MEport = DriverMarketPort);
	~CSendToMarket();

	virtual bool SendToMarket(TTradeRequest &trade_mes);
};

}	// namespace TPCE

#endif	// TXN_HARNESS_SENDTOMARKET_H
