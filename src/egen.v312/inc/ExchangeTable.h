/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the Exchange table.
*/
#ifndef EXCHANGE_TABLE_H
#define EXCHANGE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CExchangeTable : public TableTemplate<EXCHANGE_ROW>
{
	ifstream	InFile;

public:
	CExchangeTable( char *szDirName )
		: TableTemplate<EXCHANGE_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "Exchange.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CExchangeTable( )
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

#endif //EXCHANGE_TABLE_H
