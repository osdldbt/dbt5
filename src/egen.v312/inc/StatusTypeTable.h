/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the StatusType table.
*/
#ifndef STATUS_TYPE_H
#define STATUS_TYPE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CStatusTypeTable : public TableTemplate<STATUS_TYPE_ROW>
{
	ifstream	InFile;

public:
	CStatusTypeTable( char *szDirName )
		: TableTemplate<STATUS_TYPE_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "StatusType.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CStatusTypeTable( )
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

#endif //STATUS_TYPE_H
