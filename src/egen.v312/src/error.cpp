/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*/

#include "../inc/EGenUtilities_stdafx.h"

using namespace TPCE;

CMemoryErr::CMemoryErr() : CBaseErr()
{
}

CSystemErr::CSystemErr(Action eAction, char const * szLocation) : CBaseErr(szLocation)
{
#ifdef WIN32	
	m_idMsg = GetLastError();	//for Windows
#else			
	m_idMsg = errno;	//for Unix
#endif
	m_eAction = eAction;
}

CSystemErr::CSystemErr(int iError, Action eAction, char const * szLocation) : CBaseErr(szLocation)
{
	// This constructor is provided for registry functions where the function return code
	// is the error code.
	m_idMsg = iError;
	m_eAction = eAction;
}

char * CSystemErr::ErrorText()
{	
	strcpy(m_szMsg, strerror(m_idMsg));	

	return m_szMsg;
}
