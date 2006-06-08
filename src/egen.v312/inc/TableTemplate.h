/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	This template represents a general table class.
*	The type specified is the type of the table row.
*/
#ifndef TABLE_TEMPLATE_H
#define TABLE_TEMPLATE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T>
class TableTemplate
{
protected:
	T				m_row;	//private row for generation	
	TIdent			m_iLastRowNumber;	//sequential last row number
	bool			m_bMoreRecords;	//true if more records can be generated, otherwise false
	CRandom			m_rnd;	//random generator - present in all tables
public:
	//The Constructor - just initializes member variables.
	TableTemplate()
		: m_iLastRowNumber(0)
		, m_bMoreRecords(false)	//assume
	{
		memset(&m_row, 0, sizeof(m_row));	//zero the row
		m_rnd.SetSeed(RNGSeedTableDefault);
	}

	//generates the next record (row) and returns false when all records have been generated
	virtual bool GenerateNextRecord() = 0;	//abstract class

	//Returns true if all the records in this table have been generated; otherwise false.
	bool MoreRecords() {return m_bMoreRecords;}

	//Return a const reference to the data row.
	const T* GetRow() {return (const T*) &m_row; }
};

}	// namespace TPCE

#endif //TABLE_TEMPLATE_H
