/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		Larry Loen (IBM)
*
*	Description:		This file contains mappings from platform specific 
*						data types to platform indepenent data types used 
*						throughout EGen.
*
******************************************************************************/

#ifndef EGEN_STANDARD_TYPES_H
#define EGEN_STANDARD_TYPES_H

////////////////////
// Standard types //
////////////////////

// This is a template that can be used for each 
// platform type.
//
// #ifdef {platform flag}
//	// Mapping for {platform} data types.
// 	typedef {platform type}		INT8,  *PINT8;
// 	typedef {platform type}     INT16, *PINT16;
// 	typedef {platform type}     INT32, *PINT32;
// 	typedef {platform type}     INT64, *PINT64;
// 
// 	typedef {platform type}     UINT8,  *PUINT8;
// 	typedef {platform type}     UINT16, *PUINT16;
// 	typedef {platform type}     UINT32, *PUINT32;
// 	typedef {platform type}		UINT64, *PUINT64;
// #endif
//

/////////////////////////////////////////////////////
// WIN32 is predefined by the compiler             //
// for both 32-bit and 64-bit Windows platforms.   //
/////////////////////////////////////////////////////
#ifdef WIN32
#include <windows.h>	// this brings in the necessary definitions.
//
// Mapping for Windows data types.
// NOTE: The commented out types are actually provided
// (in the system file basetsd.h). They are copied here
// only for reference.

//	typedef signed char         INT8,  *PINT8;
//	typedef signed short        INT16, *PINT16;
//	typedef signed int          INT32, *PINT32;
//	typedef signed __int64      INT64, *PINT64;

//	typedef unsigned char       UINT8,  *PUINT8;
//	typedef unsigned short      UINT16, *PUINT16;
//	typedef unsigned int        UINT32, *PUINT32;
//	typedef unsigned __int64    UINT64, *PUINT64;
//

/////////////////////////////////////////////
// 64-bit integer printf format specifier  //
/////////////////////////////////////////////
#define PRId64 "I64d"

#endif // WIN32

#if defined  (__unix) || (_AIX)

#include <inttypes.h>

typedef int8_t          INT8, *PINT8;
typedef int16_t         INT16, *PINT16;
typedef int32_t         INT32, *PINT32;
typedef int64_t         INT64, *PINT64;

typedef uint8_t         UINT8,  *PUINT8;
typedef uint16_t        UINT16, *PUINT16;
typedef uint32_t        UINT32, *PUINT32;
typedef uint64_t        UINT64, *PUINT64;

/////////////////////////////////////////////
// 64-bit integer printf format specifier  //
/////////////////////////////////////////////
// Assume everyone else is a flavor of Unix, has __unix defined,
// and the 64-bit integer printf specifier is defined in <inttypes.h> as PRId64
#endif // (__unix) || (_AIX)

//////////////////////////////////////////////
// Database dependant indicator value types //
//////////////////////////////////////////////

#ifdef DB2
//
// Mapping for DB2 data types.
	typedef	UINT16	DB_INDICATOR;
//
#elif MSSQL
//
// Mapping for MSSQL data types.
	typedef	long	DB_INDICATOR;
//
#elif ORACLE
//
// Mapping for Oracle data types.
	typedef	sb2		DB_INDICATOR;
//
#else
//
// Arbitrary default just so we can compile
	typedef INT32	DB_INDICATOR;
#endif	// ORACLE

/////////////////////////////////////////////////////////
// Identifier type for all integer primary key fields. //
// Corresponds to IDENT_T metatype in TPC-E spec.      //
/////////////////////////////////////////////////////////
typedef INT64	TIdent;

#endif	// #ifndef EGEN_STANDARD_TYPES_H
