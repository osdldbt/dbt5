/*
 * CThreadErr.h
 * Class for thread errors
 *
 * 2006 Rilson Nascimento
 *
 * 05 August 2006
 */

#ifndef THREAD_ERR_H
#define THREAD_ERR_H

#include <error.h>

namespace TPCE
{

#define ERR_TYPE_THREAD		14		//thread error

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

#endif	//THREAD_ERR
