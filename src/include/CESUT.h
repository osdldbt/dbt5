/*
 * CESUT.h
 * CE (Customer Emulator) - SUT (Brokerage House) connection class
 *
 * 2006 Rilson Nascimento
 *
 * 28 July 2006
 */

#ifndef CE_SUT_H
#define CE_SUT_H

#include "CESUTInterface.h"

namespace TPCE
{

class CCESUT : public CCESUTInterface
{
	CSocket			m_Socket;
	TMsgDriverBrokerage	m_Request;

private:
	void ConnectRunTxnAndLog(void);
	
public:
	CCESUT();
	~CCESUT(void);

	virtual bool BrokerVolume( PBrokerVolumeTxnInput pTxnInput );			// return whether it was successful
	virtual bool CustomerPosition( PCustomerPositionTxnInput pTxnInput );		// return whether it was successful
	virtual bool MarketWatch( PMarketWatchTxnInput pTxnInput );			// return whether it was successful
	virtual bool SecurityDetail( PSecurityDetailTxnInput pTxnInput );		// return whether it was successful
	virtual bool TradeLookup( PTradeLookupTxnInput pTxnInput );			// return whether it was successful
	virtual bool TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner );
											// return whether it was successful
	virtual bool TradeStatus( PTradeStatusTxnInput pTxnInput );			// return whether it was successful
	virtual bool TradeUpdate( PTradeUpdateTxnInput pTxnInput );			// return whether it was successful

	// RunTxn
	template<typename T, typename T2> inline bool RunTxn(eTxnType TxnType, T* pRequestInput, T2* pTxnInput)
	{
		memset(&m_Request, 0, sizeof(m_Request));
		m_Request.TxnType = TxnType;
		memcpy( pRequestInput, pTxnInput, sizeof( T ));
	
		ConnectRunTxnAndLog();
		return true;
	}

};

}	// namespace TPCE

#endif	// CE_SUT_H
