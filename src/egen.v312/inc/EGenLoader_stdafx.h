/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef EGEN_LOADER_STDAFX_H
#define EGEN_LOADER_STDAFX_H

// Note: don't use these. Pass the corresponding define on the command-line for the compiler.
//#define COMPILE_ODBC_LOAD	//define this to compile for ODBC load
//#define COMPILE_FLAT_FILE_LOAD	//define this to compile flat file output loader library

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

#ifdef COMPILE_ODBC_LOAD
#ifdef WIN32
#include <windows.h>
#endif //WIN32
// ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>
#endif //COMPILE_ODBC_LOAD

// TODO: reference additional headers your program requires here
using namespace std;

#ifdef COMPILE_PGSQL_LOAD
#include <pqxx/pqxx>
using namespace pqxx;
#endif //COMPILE_PGSQL_LOAD

#include "EGenTables_stdafx.h"
#include "EGenBaseLoader_stdafx.h"
#include "EGenGenerateAndLoad_stdafx.h"
// Include one or more load types.
#include "EGenNullLoader_stdafx.h"
#ifdef COMPILE_FLAT_FILE_LOAD
#include "FlatFileLoad_stdafx.h"
#endif
#ifdef COMPILE_ODBC_LOAD
#include "win/ODBCLoad_stdafx.h"
#endif
#ifdef COMPILE_PGSQL_LOAD
#include "pgsql/PGSQLLoad.h"
#endif

// Generic Error Codes
#define ERROR_BAD_OPTION 1
#define ERROR_INPUT_FILE 2
#define ERROR_INVALID_OPTION_VALUE 3
#endif	// #ifndef EGEN_LOADER_STDAFX_H
