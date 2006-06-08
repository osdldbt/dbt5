/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the CommissionRate table.
*/
#ifndef COMMISSION_RATE_TABLE_H
#define COMMISSION_RATE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CCommissionRateTable : public TableTemplate<COMMISSION_RATE_ROW>
{
	ifstream	InFile;

public:
	CCommissionRateTable( char *szDirName )
		: TableTemplate<COMMISSION_RATE_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "CommissionRate.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CCommissionRateTable( )
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

#endif //COMMISSION_RATE_TABLE_H
