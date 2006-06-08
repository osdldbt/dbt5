/******************************************************************************
*	(c) Copyright 2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Implementation of the Company Competitor input file
*						that scales with the database size.
*
******************************************************************************/

#ifndef COMPANY_COMPETITOR_FILE_H
#define COMPANY_COMPETITOR_FILE_H

namespace TPCE
{

class CCompanyCompetitorFile : public CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>
{	
	// Configured and active number of companies in the database.
	// Depends on the configured and active number of customers.
	//
	TIdent	m_iConfiguredCompanyCompetitorCount;
	TIdent	m_iActiveCompanyCompetitorCount;

	// Number of base companies (=rows in Company.txt input file).
	//
	UINT	m_iBaseCompanyCount;

public:

	// Constructor.
	//
	CCompanyCompetitorFile(const char *szListFile)
	: CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(szListFile)	
	, m_iConfiguredCompanyCompetitorCount(0)	// must be initialized later
	, m_iActiveCompanyCompetitorCount(0)		// must be initialized later
	{
		TCompanyLimits		CompanyInputFileLimits;
	
		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}
	
	// Constructor.
	//
	CCompanyCompetitorFile(const string &str)
	: CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(str)	
	, m_iConfiguredCompanyCompetitorCount(0)	// must be initialized later
	, m_iActiveCompanyCompetitorCount(0)		// must be initialized later
	{
		TCompanyLimits		CompanyInputFileLimits;
	
		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}

	// Constructor.
	//
	CCompanyCompetitorFile(const char *szListFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(szListFile)	
	{
		TCompanyLimits		CompanyInputFileLimits;

		SetConfiguredCompanyCompetitorCount(iConfiguredCustomerCount);		
		SetActiveCompanyCompetitorCount(iActiveCustomerCount);		
	
		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}
	
	// Constructor.
	//
	CCompanyCompetitorFile(const string &str, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(str)	
	{	
		TCompanyLimits		CompanyInputFileLimits;

		SetConfiguredCompanyCompetitorCount(iConfiguredCustomerCount);		
		SetActiveCompanyCompetitorCount(iActiveCustomerCount);		

		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}

	// Delayed initialization of the configured number of company competitors.
	// Made available for situations when the configured number of customers
	// is not known at construction time.
	//
	void SetConfiguredCompanyCompetitorCount(TIdent iConfiguredCustomerCount)
	{
		m_iConfiguredCompanyCompetitorCount = CalculateCompanyCompetitorCount(iConfiguredCustomerCount);
	}

	// Delayed initialization of the active number of company competitors.
	// Made available for situations when the active number of customers
	// is not known at construction time.
	//
	void SetActiveCompanyCompetitorCount(TIdent iActiveCustomerCount)
	{
		m_iActiveCompanyCompetitorCount = CalculateCompanyCompetitorCount(iActiveCustomerCount);
	}

	// Calculate company competitor count for the specified number of customers.
	// Sort of a static method. Used in parallel generation of company related tables.
	//
	TIdent CalculateCompanyCompetitorCount(TIdent iCustomerCount)
	{
		return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
	}

	// Calculate the first company competitor id (0-based) for the specified customer id
	//
	TIdent CalculateStartFromCompanyCompetitor(TIdent iStartFromCustomer)
	{
		return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
	}

	// Return company id for the specified row.
	// Index can exceed the size of the Company Competitor input file.
	//
	TIdent GetCompanyId(TIdent iIndex)
	{
		// Index wraps around every 15000 companies.
		//
		return m_list[ (int)(iIndex % m_list.size()) ].CP_CO_ID
				+ iIndex / m_list.size() * m_iBaseCompanyCount;
	}

	// Return company competitor id for the specified row.
	// Index can exceed the size of the Company Competitor input file.
	//
	TIdent GetCompanyCompetitorId(TIdent iIndex)
	{
		// Index wraps around every 5000 companies.
		//
		return m_list[ (int)(iIndex % m_list.size()) ].CP_COMP_CO_ID 
				+ iIndex / m_list.size() * m_iBaseCompanyCount;
	}

	// Return industry id for the specified row.
	// Index can exceed the size of the Company Competitor input file.
	//
	char* GetIndustryId(TIdent iIndex)
	{
		// Index wraps around every 5000 companies.
		//
		return m_list[ (int)(iIndex % m_list.size()) ].CP_IN_ID;
	}

	// Return the number of company competitors in the database for
	// the configured number of customers.
	//	
	TIdent GetConfiguredCompanyCompetitorCount()
	{
		return m_iConfiguredCompanyCompetitorCount;
	}

	// Return the number of company competitors in the database for
	// the active number of customers.
	//	
	TIdent GetActiveCompanyCompetitorCount()
	{
		return m_iActiveCompanyCompetitorCount;
	}

	// Overload GetRecord to wrap around indices that 
	// are larger than the flat file
	//
	TCompanyCompetitorInputRow*	GetRecord(TIdent index) { return &m_list[(int)(index % m_list.size())]; };
};

}	// namespace TPCE

#endif // COMPANY_COMPETITOR_FILE_H

