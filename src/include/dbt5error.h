/*
 * dbt5Err.h
 *
 * 2006 Rilson Nascimento
 *
 * 07 August 2006
 */

#ifndef DBT5_ERR_H
#define DBT5_ERR_H

#include <error.h>
#include <string>

namespace TPCE
{

#define ERR_TYPE_SOCKET		13		// socket error
#define ERR_TYPE_THREAD		14		// thread error
#define ERR_TYPE_PQXX		15		// libpqxx error
#define ERR_TYPE_WRONGTXN	16		// wrong txn type
#define ERROR_LOG_NAME 		"error.log"
#define CE_MIX_LOG_NAME		"ce_mix.log"
#define MEE_MIX_LOG_NAME	"mee_mix.log"

static std::string PGSQL_SERIALIZE_ERROR = "ERROR:  could not serialize access due to concurrent update";

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
	
	Action GetAction() { return m_eAction; };
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

class CThreadErr : public CBaseErr
{
public:
	enum Action
	{
		ERR_THREAD_ATTR_INIT = 0,
		ERR_THREAD_ATTR_DETACH,
		ERR_THREAD_CREATE,
		ERR_THREAD_JOIN
	};

private:
	Action		m_eAction;

public:
	CThreadErr(Action eAction, char const * szLoc = NULL)
	: CBaseErr(szLoc)
	{
		m_eAction = eAction;
	};

	~CThreadErr()
	{
	}
	
	int ErrorType() { return ERR_TYPE_THREAD; };

	char *ErrorText()
	{
		static	char *szErrMsg[4] = {
			(char*)"pthread_attr_init failed",
			(char*)"pthread_attr_setdetachstate failed",
			(char*)"pthread_create failed",
			(char*)"error join terminal thread",
		};

		return szErrMsg[m_eAction];
	};
	
};

}	//namespace TPCE

#endif	//DBT5_ERR_H
