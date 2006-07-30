/*
 * CSocketErr.h
 * Error class for Socket class
 *
 * 2006 Rilson Nascimento
 *
 * 22 June 2006
 */

#ifndef SOCKET_ERR_H
#define SOCKET_ERR_H

#include <string>

namespace TPCE
{

class CSocketErr
{ 
public:
	enum Action
	{
		ERR_SOCKET_ACCEPT = 0,
		ERR_SOCKET_SINPORT,
		ERR_SOCKET_HOSTBYNAME,
		ERR_SOCKET_HOSTBYADDR,
		ERR_SOCKET_CREATE,
		ERR_SOCKET_CONNECT,
		ERR_SOCKET_BIND,
		ERR_SOCKET_LISTEN,
		ERR_SOCKET_RESOLVPROTO,
		ERR_SOCKET_INETPTON,
		ERR_SOCKET_RECV,
		ERR_SOCKET_CLOSED,
		ERR_SOCKET_RECVPARTIAL,
		ERR_SOCKET_SEND,
		ERR_SOCKET_SENDPARTIAL
	};

private:
	Action		m_eAction;
	std::string	m_sLocation;

public:

	CSocketErr(Action eAction, std::string sLocation)
	:m_eAction(eAction)
	,m_sLocation(sLocation)	{};

	~CSocketErr();
	
	std::string GetLocation(void) { return m_sLocation; };

	std::string ErrorText(void)
	{
		static	std::string  sErrMsg[15] = {
			"Can't accept client connection",
			"Please specify port on which server listen for request",
			"Please specify correct hostname of box where server is running",
			"Please specify correct IP of box where server is running",
			"Can't create socket",
			"Can't connect to server socket",
			"Can't bind socket",
			"Can't listen on socket",
			"Error in getprotobyname",
			"inet_pton - Error in converting string to network address",
			"cannot receive data",
			"socket closed on operation",
			"did not receive all data",
			"cannot send data",
			"did not send all data"
		};

		return sErrMsg[m_eAction];
	};
	
};

}	//namespace TPCE

#endif	//SOCKET_ERR_H
