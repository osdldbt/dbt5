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

#include <error.h>

namespace TPCE
{

#define ERR_TYPE_SOCKET		13		//socket error

class CSocketErr : public CBaseErr
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

public:
	CSocketErr(Action eAction, char const * szLoc = NULL)
	: CBaseErr(szLoc)
	{
		m_eAction = eAction;
	};

	~CSocketErr()
	{
	}
	
	int ErrorType() { return ERR_TYPE_SOCKET; };

	char *ErrorText()
	{
		static	char *szErrMsg[15] = {
			(char*)"Can't accept client connection",
			(char*)"Please specify port on which server listen for request",
			(char*)"Please specify correct hostname of box where server is running",
			(char*)"Please specify correct IP of box where server is running",
			(char*)"Can't create socket",
			(char*)"Can't connect to server socket",
			(char*)"Can't bind socket",
			(char*)"Can't listen on socket",
			(char*)"Error in getprotobyname",
			(char*)"inet_pton - Error in converting string to network address",
			(char*)"cannot receive data",
			(char*)"socket closed on operation",
			(char*)"did not receive all data",
			(char*)"cannot send data",
			(char*)"did not send all data"
		};

		return szErrMsg[m_eAction];
	};
	
};

}	//namespace TPCE

#endif	//SOCKET_ERR_H
