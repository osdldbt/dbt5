/*
 * TxnHarnessSendToMarket.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;

char* addr = "localhost";

CSendToMarket::CSendToMarket()
{
}

CSendToMarket::~CSendToMarket()
{
}

bool CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
	// connect to the Market Exchange
	m_Socket.Connect(addr, DriverMarketPort);

	// send Trade Request to MEE
	m_Socket.Send(reinterpret_cast<void*>(&trade_mes), sizeof(TTradeRequest));

	// close connection
	m_Socket.CloseAccSocket();

	return true;
}

