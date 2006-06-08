/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the Industry table.
*/
#ifndef INDUSTRY_TABLE_H
#define INDUSTRY_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CIndustryTable : public TableTemplate<INDUSTRY_ROW>
{
	ifstream	InFile;

public:
	CIndustryTable( char *szDirName )
		: TableTemplate<INDUSTRY_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "Industry.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CIndustryTable( )
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

#endif //INDUSTRY_TABLE_H
