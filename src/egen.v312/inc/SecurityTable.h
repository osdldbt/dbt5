/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing the securities table.
*/
#ifndef SECURITY_TABLE_H
#define SECURITY_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

const double	fS_NUM_OUTMin = 4000000.0;
const double	fS_NUM_OUTMax = 9500000000.0;

const double	fS_PEMin = 1.0;
const double	fS_PEMax = 120.0;

const double	fS_DIVIDNonZeroMin = 0.01;
const double	fS_DIVIDMax = 10.0;

const double	fS_YIELDNonZeroMin = 0.01;
const double	fS_YIELDMax = 120.0;

const int	iWeeksPerYear = 52;
const int	iDaysPerWeek = 7;

const int	iPercentCompaniesWithNonZeroDividend = 60;

class CSecurityTable : public TableTemplate<SECURITY_ROW>
{
	TIdent			m_iSecurityCount;
	TIdent			m_iStartFromSecurity;
	CCompanyFile*	m_CompanyFile;
	CSecurityFile*	m_SecurityFile;
	CDateTime		m_date;
	int				m_iCurrentDayNo;
	int				m_iJan1_1900DayNo;
	int				m_iJan2_2000DayNo;

	/*
	*	SECURITY table row generation
	*/
	void GenerateSecurityRow()
	{
		int	iStartDayNo, iExchangeDayNo, i52HighDayNo, i52LowDayNo;

		strncpy(m_row.S_ST_ID, m_SecurityFile->GetRecord(m_iLastRowNumber)->S_ST_ID, sizeof(m_row.S_ST_ID)-1);
		strncpy(m_row.S_ISSUE, m_SecurityFile->GetRecord(m_iLastRowNumber)->S_ISSUE, sizeof(m_row.S_ISSUE)-1);
		
		CreateName(m_iLastRowNumber, m_row.S_NAME, sizeof(m_row.S_NAME));

		m_SecurityFile->CreateSymbol(m_iLastRowNumber, m_row.S_SYMB, sizeof(m_row.S_SYMB));
		strncpy(m_row.S_EX_ID, m_SecurityFile->GetRecord(m_iLastRowNumber)->S_EX_ID, sizeof(m_row.S_EX_ID)-1);
		m_row.S_CO_ID = m_SecurityFile->GetCompanyId(m_iLastRowNumber);
		m_row.S_NUM_OUT = m_rnd.RndDoubleIncrRange(fS_NUM_OUTMin, fS_NUM_OUTMax, 1.0);
		
		iStartDayNo = m_rnd.RndIntRange(m_iJan1_1900DayNo, m_iJan2_2000DayNo);	//generate random date
		m_row.S_START_DATE.Set(iStartDayNo);
		iExchangeDayNo = m_rnd.RndIntRange(iStartDayNo, m_iJan2_2000DayNo);
		m_row.S_EXCH_DATE.Set(iExchangeDayNo);
		m_row.S_PE = m_rnd.RndDoubleIncrRange(fS_PEMin, fS_PEMax, 0.01);
		//iExchangeDayNo contains S_EXCH_DATE date in days.

		//52 week high  - selected from upper half of security price range
		m_row.S_52WK.HIGH = (float)m_rnd.RndDoubleIncrRange( fMinSecPrice + (( fMaxSecPrice - fMinSecPrice) / 2 ), fMaxSecPrice, 0.01);
		i52HighDayNo = m_rnd.RndIntRange(m_iCurrentDayNo - iDaysPerWeek * iWeeksPerYear, m_iCurrentDayNo);
		m_row.S_52WK.HIGH_DATE.Set(i52HighDayNo);

		//52 week low - selected from the minimum security price up to the 52wk high
		m_row.S_52WK.LOW = (float)m_rnd.RndDoubleIncrRange( fMinSecPrice, m_row.S_52WK.HIGH, 0.01 );
		i52LowDayNo = m_rnd.RndIntRange(m_iCurrentDayNo - iDaysPerWeek * iWeeksPerYear, m_iCurrentDayNo);
		m_row.S_52WK.LOW_DATE.Set(i52LowDayNo);

		m_row.S_52WK.iIndicator = OLTP_VALUE_IS_NOT_NULL;

		m_date.Set(m_iCurrentDayNo);	//set the current date back

		if (m_rnd.RndPercent( iPercentCompaniesWithNonZeroDividend ))
		{
			m_row.S_YIELD = m_rnd.RndDoubleIncrRange(fS_YIELDNonZeroMin, fS_YIELDMax, 0.01);
			m_row.S_DIVIDEND = m_rnd.RndDoubleIncrRange(( m_row.S_YIELD * 0.20 ), ( m_row.S_YIELD * 0.30 ), 0.01);
		}
		else
		{
			m_row.S_DIVIDEND = 0.0;
			m_row.S_YIELD = 0.0;
		}
	}
		

public:
	CSecurityTable(	CInputFiles inputFiles,
					TIdent		iCustomerCount, 
					TIdent		iStartFromCustomer)
		: TableTemplate<SECURITY_ROW>()
		, m_CompanyFile(inputFiles.Company)
		, m_SecurityFile(inputFiles.Securities)		
	{
		m_iCurrentDayNo = m_date.DayNo();	//get current date
		m_iJan1_1900DayNo = CDateTime::YMDtoDayno(1900, 1, 1);//find out number of days for Jan-1-1900
		m_iJan2_2000DayNo = CDateTime::YMDtoDayno(2000, 1, 2);//find out number of days for Jan-2-2000

		m_iSecurityCount = m_SecurityFile->CalculateSecurityCount(iCustomerCount);
		m_iStartFromSecurity = m_SecurityFile->CalculateStartFromSecurity(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromSecurity;
	};

	bool GenerateNextRecord()
	{
		GenerateSecurityRow();

		++m_iLastRowNumber;

		//Update state info
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromSecurity + m_iSecurityCount);

		return (MoreRecords());
	}	

	void CreateName(	TIdent	iIndex,		// row number
						char*	szOutput,	// output buffer
						int		iOutputLen)	// size of the output buffer (including null)
	{
		char	CompanyName[cCO_NAME_len+1];

		m_CompanyFile->CreateName( m_SecurityFile->GetCompanyIndex( iIndex ), CompanyName, sizeof( CompanyName ));
		sprintf( szOutput, "%s of %s", m_SecurityFile->GetRecord(iIndex)->S_ISSUE, CompanyName );
	}

};

}	// namespace TPCE

#endif //SECURITY_TABLE_H
