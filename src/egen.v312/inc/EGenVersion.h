/******************************************************************************
*	(c) Copyright 2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Versioning information for the EGen package.
*						Updated on every release.
*
******************************************************************************/

#ifndef EGEN_VERSION_H
#define EGEN_VERSION_H

namespace TPCE
{

extern "C" 
{

	// Retrieve major, minor, revision, and beta level numbers for EGen.
// For example, v3.10 beta 1 has:
//	major		3
//	minor		10
//	revision	0
//	beta level	1
// v3.10 release has:
//	major		3
//	minor		10
//	revision	0
//	beta level	0
//
void GetEGenVersion(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel);

// Return versioning information formated as a string
//
// Note: requires output buffer at least 64 characters long, or nothing will be returned.
//
void GetEGenVersionString(char* szOutput, INT32 iOutputBufferLen);

// Output EGen versioning information on stdout
//
void PrintEGenVersion();

// Return the date/time when the EGen versioning information was last updated.
//
void GetEGenVersionUpdateTimestamp(char* szOutput, INT32 iOutputBufferLen);

}

}

#endif	// #ifndef EGEN_VERSION_H
