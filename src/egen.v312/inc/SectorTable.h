/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the Sector table.
*/
#ifndef SECTOR_TABLE_H
#define SECTOR_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CSectorTable : public TableTemplate<SECTOR_ROW>
{
	ifstream	InFile;

public:
	CSectorTable( char *szDirName )
		: TableTemplate<SECTOR_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "Sector.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CSectorTable( )
	{
		InFile.close();
	};

	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		if( InFile.good() )
		{
			m_row.Load(InFile);
		}

		return ( InFile.eof() );
	}
};

}	// namespace TPCE

#endif //SECTOR_TABLE_H
