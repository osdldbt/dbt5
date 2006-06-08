/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef EGEN_UTILITIES_STDAFX_H
#define EGEN_UTILITIES_STDAFX_H

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
#include <stddef.h>
#include <stdio.h>
#ifdef WIN32		//for Windows platform
#include <windows.h>
#else
#include <errno.h>	//for Unix
#include <sys/time.h>	//for gettimeofday() on Linux
#endif
#include <time.h>
#include <assert.h>
#include <list>

using namespace std;

// TODO: reference additional headers your program requires here
#include "EGenStandardTypes.h"
#include "DateTime.h"
#include "Random.h"
#include "error.h"
#include "TableConsts.h"
#include "MiscConsts.h"
#include "FixedMap.h"
#include "FixedArray.h"
// Include platform-dependent syncronization lock object
#ifdef WIN32
#include "win/SyncLock.h"	// Windows implementation
//#elif DEFINED( LINUX )
//#include "linux/SyncLock.h"	// Linux implementation
#else
#include "SyncLockInterface.h"	//empty stub
#endif

#include "Queue.h"
#include "RNGSeeds.h"
#include "EGenVersion.h"
#include "Money.h"

#endif	// #ifndef EGEN_UTILITIES_STDAFX_H
