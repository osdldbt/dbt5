/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the ZIP_CODE table.
*/
#ifndef ZIP_CODE_TABLE_H
#define ZIP_CODE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CZipCodeTable : public TableTemplate<ZIP_CODE_ROW>
{
private:
	TZipCodeFile*		m_ZipCode;
	TZipCodeInputRow*	m_NextRow;
	INT32				m_NextUniqueRecordID;

public:
	CZipCodeTable( CInputFiles inputFiles )
		: TableTemplate<ZIP_CODE_ROW>()
		, m_ZipCode( inputFiles.ZipCode )
		, m_NextUniqueRecordID( 0 )
	{
		// No body for constructor.
	};


	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		if( m_NextUniqueRecordID < m_ZipCode->RecordCount( ))
		{
			strncpy( m_row.ZC_CODE, (m_ZipCode->GetRecordByPassKey( m_NextUniqueRecordID ))->ZC_CODE, sizeof( m_row.ZC_CODE ) - 1 );
			strncpy( m_row.ZC_TOWN, (m_ZipCode->GetRecordByPassKey( m_NextUniqueRecordID ))->ZC_TOWN, sizeof( m_row.ZC_TOWN ) - 1 );
			strncpy( m_row.ZC_DIV, (m_ZipCode->GetRecordByPassKey( m_NextUniqueRecordID ))->ZC_DIV, sizeof( m_row.ZC_DIV ) - 1 );
			m_NextUniqueRecordID++;
			return( true );
		}
		else
		{
			return ( false );
		}
	}
};

}	// namespace TPCE

#endif //ZIP_CODE_TABLE_H
