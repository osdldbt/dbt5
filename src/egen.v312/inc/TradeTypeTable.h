/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the TradeType table.
*/
#ifndef TRADE_TYPE_TABLE_H
#define TRADE_TYPE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CTradeTypeTable : public TableTemplate<TRADE_TYPE_ROW>
{
	ifstream	InFile;

public:
	CTradeTypeTable( char *szDirName )
		: TableTemplate<TRADE_TYPE_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "TradeType.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CTradeTypeTable( )
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

#endif //TRADE_TYPE_TABLE_H
