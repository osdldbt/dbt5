/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the Charge table.
*/
#ifndef CHARGE_TABLE_H
#define CHARGE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CChargeTable : public TableTemplate<CHARGE_ROW>
{
	ifstream	InFile;

public:
	CChargeTable( char *szDirName )	// input directory where to find the flat file
		: TableTemplate<CHARGE_ROW>()
	{
		char szFileName[iMaxPath];

		strncpy(szFileName, szDirName, sizeof(szFileName));
		strncat(szFileName, "Charge.txt", sizeof(szFileName) - strlen(szDirName) - 1);

		InFile.open( szFileName );
	};

	~CChargeTable( )
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

#endif //CHARGE_TABLE_H
