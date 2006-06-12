/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
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
