/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing the CompanyCompetitor table.
*/
#ifndef COMPANY_COMPETITOR_TABLE_H
#define COMPANY_COMPETITOR_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CCompanyCompetitorTable : public TableTemplate<COMPANY_COMPETITOR_ROW>
{
	CCompanyCompetitorFile*		m_pCompanyCompetitorFile;
	TIdent						m_iCompanyCompetitorCount;
	TIdent						m_iStartFromCompanyCompetitor;

public:
	CCompanyCompetitorTable(CInputFiles inputFiles,
							TIdent		iCustomerCount, 
							TIdent		iStartFromCustomer)
	: TableTemplate<COMPANY_COMPETITOR_ROW>()
	, m_pCompanyCompetitorFile(inputFiles.CompanyCompetitor)
	{
		m_iCompanyCompetitorCount = m_pCompanyCompetitorFile->CalculateCompanyCompetitorCount(iCustomerCount);
		m_iStartFromCompanyCompetitor = m_pCompanyCompetitorFile->CalculateStartFromCompanyCompetitor(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromCompanyCompetitor;
	};	

	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		m_row.CP_CO_ID = m_pCompanyCompetitorFile->GetCompanyId(m_iLastRowNumber);

		m_row.CP_COMP_CO_ID = m_pCompanyCompetitorFile->GetCompanyCompetitorId(m_iLastRowNumber);

		strncpy(m_row.CP_IN_ID, 
				m_pCompanyCompetitorFile->GetIndustryId(m_iLastRowNumber),
				sizeof(m_row.CP_IN_ID));

		++m_iLastRowNumber;

		m_bMoreRecords = m_iLastRowNumber < m_iStartFromCompanyCompetitor + m_iCompanyCompetitorCount;

		return (MoreRecords());
	}
};

}	// namespace TPCE

#endif //COMPANY_COMPETITOR_TABLE_H
