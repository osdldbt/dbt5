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

#include "../inc/EGenUtilities_stdafx.h"

namespace TPCE
{

// Modify these constants whenever EGen version changes.
//
static INT32	iEGenMajorVersion	= 3;
static INT32	iEGenMinorVersion	= 12;
static INT32	iEGenRevisionNumber	= 0;	// third-tier revision number
static INT32	iEGenBetaLevel		= 0;


extern "C"
{
// Retrieve major, minor, revision, and beta level numbers for EGen.
//
void GetEGenVersion(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel)
{
	iMajorVersion = iEGenMajorVersion;
	iMinorVersion = iEGenMinorVersion;
	iRevisionNumber = iEGenRevisionNumber;
	iBetaLevel = iEGenBetaLevel;
}
// Return versioning information formated as a string
//
void GetEGenVersionString(char* szOutput, INT32 iOutputBufferLen)
{
	int	iLen;

	if (iOutputBufferLen < 64)
		return;	// buffer too small - require at least 64 characters

	iLen = sprintf(szOutput, "EGen v%d.%d", iEGenMajorVersion, iEGenMinorVersion);
	
	if (iEGenRevisionNumber != 0)
	{
		iLen += sprintf(&szOutput[iLen], " (rev. %d)", iEGenRevisionNumber);
	}

	if (iEGenBetaLevel != 0)
	{
		iLen+= sprintf(&szOutput[iLen], " beta %d", iEGenBetaLevel);
	}
}

// Output EGen versioning information on stdout
//
void PrintEGenVersion()
{
	char	szVersion[128];

	GetEGenVersionString(szVersion, sizeof(szVersion)-1);

	printf("%s\n", szVersion);
}

// Return the date/time when the EGen versioning information was last updated.
//
void GetEGenVersionUpdateTimestamp(char* szOutput, INT32 iOutputBufferLen)
{
	strncpy(szOutput, __DATE__" "__TIME__, iOutputBufferLen);
}

}

}	// namespace TPC-E
