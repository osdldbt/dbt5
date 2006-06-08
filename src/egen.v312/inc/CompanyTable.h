/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing the Company table.
*/
#ifndef COMPANY_TABLE_H
#define COMPANY_TABLE_H

namespace TPCE
{

const int iCEOMult=1000;	//for generating CEO name

class CCompanyTable : public TableTemplate<COMPANY_ROW>
{
	CCompanyFile*		m_CompanyFile;
	TCompanySPRateFile*	m_CompanySPRateFile;
	CPerson				m_person;	//for CEO
	CDateTime			m_date;
	TIdent				m_iCO_AD_ID_START;	//starting address id for companies
	int					m_iJan1_1800_DayNo;
	int					m_iJan2_2000_DayNo;
	int					m_iCurrentDayNo;
	TIdent				m_iCompanyCount;
	TIdent				m_iStartFromCompany;

	int GetCompanySPRateThreshold()
	{
		RNGSEED	OldSeed;
		int		iCompanySPRateThreshold;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseSPRate, m_row.CO_ID ));
		iCompanySPRateThreshold = m_rnd.RndIntRange(0, m_CompanySPRateFile->GetGreatestKey()-1);
		m_rnd.SetSeed( OldSeed );
		return( iCompanySPRateThreshold );
	}

	void GenerateCompanySPRate(void)
	{
		int iThreshold = GetCompanySPRateThreshold();

		//Select the row in the input file
		const TCompanySPRateInputRow* pCompanySPRateInputRow = m_CompanySPRateFile->GetRecord(iThreshold);
		strncpy( m_row.CO_SP_RATE, pCompanySPRateInputRow->CO_SP_RATE, sizeof(m_row.CO_SP_RATE));
	}
	
public:
	CCompanyTable(	CInputFiles inputFiles, 
					TIdent		iCustomerCount, 
					TIdent		iStartFromCustomer)
		: TableTemplate<COMPANY_ROW>()
		, m_CompanyFile(inputFiles.Company)
		, m_CompanySPRateFile(inputFiles.CompanySPRate)
		, m_person(inputFiles)
	{	
		m_iJan1_1800_DayNo = CDateTime::YMDtoDayno(1800, 1, 1);	//days number for Jan 1, 1800
		m_iJan2_2000_DayNo = CDateTime::YMDtoDayno(2000, 1, 2);	//days number for Jan 2, 2000
		m_iCurrentDayNo = m_date.DayNo();	//today's days number				

		m_iCompanyCount = m_CompanyFile->CalculateCompanyCount(iCustomerCount);
		m_iStartFromCompany = m_CompanyFile->CalculateStartFromCompany(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromCompany;
		// Start Company addresses immediately after Exchange addresses,
		// and company addresses for prior companies
		m_iCO_AD_ID_START = inputFiles.Exchange->GetSize() + m_iStartFromCompany;
	};

	// Generate and store state information for the next CO_ID; return false if all ids are generated
	bool GenerateNextCO_ID()
	{
		++m_iLastRowNumber;
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromCompany + m_iCompanyCount);

		return (MoreRecords());
	}

	// Return the current CO_ID
	TIdent GetCurrentCO_ID()
	{
		return (m_CompanyFile->GetCompanyId(m_iLastRowNumber));
	}

	bool GenerateNextRecord()
	{
		int	iFoundDayNo;				

		m_row.CO_ID = GetCurrentCO_ID();

		strncpy(m_row.CO_ST_ID, 
				m_CompanyFile->GetRecord(m_iLastRowNumber)->CO_ST_ID, 
				sizeof(m_row.CO_ST_ID)-1);
		
		m_CompanyFile->CreateName(	m_iLastRowNumber, 
									m_row.CO_NAME, 
									sizeof(m_row.CO_NAME));

		strncpy(m_row.CO_IN_ID, 
				m_CompanyFile->GetRecord(m_iLastRowNumber)->CO_IN_ID, 
				sizeof(m_row.CO_IN_ID)-1);

		GenerateCompanySPRate();

		sprintf(m_row.CO_CEO, "%s %s", 
			m_person.GetFirstName(iCEOMult*m_row.CO_ID),
			m_person.GetLastName(iCEOMult*m_row.CO_ID));

		strncpy(m_row.CO_DESC, 
				m_CompanyFile->GetRecord(m_iLastRowNumber)->CO_DESC, 
				sizeof(m_row.CO_DESC)-1);

		m_row.CO_AD_ID = ++m_iCO_AD_ID_START;

		iFoundDayNo = m_rnd.RndIntRange(m_iJan1_1800_DayNo, m_iJan2_2000_DayNo);

		m_row.CO_OPEN_DATE.Set(iFoundDayNo);			

		//Update state info		
		return GenerateNextCO_ID();
	}	
};

}	// namespace TPCE

#endif //COMPANY_TABLE_H
