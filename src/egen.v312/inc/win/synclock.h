/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

#ifndef SYNCLOCK_H
#define SYNCLOCK_H

/*
*	Windows implementation of a syncronization lock that uses Win32 mutex.
*/

namespace TPCE
{

class CSyncLock
{
	CRITICAL_SECTION	m_CriticalSection;
public:

	// Constructor
	CSyncLock()
	{
		InitializeCriticalSection(&m_CriticalSection);	// do not acquire initial ownership		
	}

	// Destructor
	~CSyncLock()
	{		
		DeleteCriticalSection(&m_CriticalSection);
	}

	// Acquire lock
	void ClaimLock()
	{
		EnterCriticalSection(&m_CriticalSection);
	}

	// Release mutex
	void ReleaseLock()
	{
		LeaveCriticalSection(&m_CriticalSection);
	}
};

}	// namespace TPCE

#endif	// #ifndef SYNCLOCK_H
