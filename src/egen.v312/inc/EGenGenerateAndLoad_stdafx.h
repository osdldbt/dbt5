/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef EGEN_GENERATE_AND_LOAD_STDAFX_H
#define EGEN_GENERATE_AND_LOAD_STDAFX_H

#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
#include <stdio.h>
#include <assert.h>
#include <fstream>
#include <set>
#include <map>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>

using namespace std;

#include "EGenTables_stdafx.h"
#include "EGenBaseLoader_stdafx.h"
#include "EGenGenerateAndLoadBaseOutput.h"
#include "EGenGenerateAndLoadStandardOutput.h"
#include "EGenGenerateAndLoad.h"

#endif	// #ifndef EGEN_GENERATE_AND_LOAD_STDAFX_H
