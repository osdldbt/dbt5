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
*	Description:		Implementation of the Company input file that scales
*						with the database size.
*
******************************************************************************/

#ifndef COMPANY_FILE_H
#define COMPANY_FILE_H

namespace TPCE
{

class CCompanyFile	: public CFlatFile<TCompanyInputRow, TCompanyLimits>
{	
	// Configured and active number of companies in the database.
	// Depends on the configured and active number of customers.
	//
	TIdent	m_iConfiguredCompanyCount;
	TIdent	m_iActiveCompanyCount;

public:

	// Constructor.
	//
	CCompanyFile(const char *szListFile)
	: CFlatFile<TCompanyInputRow, TCompanyLimits>(szListFile)	
	, m_iConfiguredCompanyCount(0)	// must be initialized later
	, m_iActiveCompanyCount(0)	// must be initialized later
	{		
	}
	
	// Constructor.
	//
	CCompanyFile(const string &str)
	: CFlatFile<TCompanyInputRow, TCompanyLimits>(str)	
	, m_iConfiguredCompanyCount(0)	// must be initialized later
	, m_iActiveCompanyCount(0)	// must be initialized later
	{		 
	}

	// Constructor.
	//
	CCompanyFile(const char *szListFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TCompanyInputRow, TCompanyLimits>(szListFile)	
	{
		SetConfiguredCompanyCount(iConfiguredCustomerCount);
		SetActiveCompanyCount(iActiveCustomerCount);
	}
	
	// Constructor.
	//
	CCompanyFile(const string &str, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TCompanyInputRow, TCompanyLimits>(str)	
	{
		SetConfiguredCompanyCount(iConfiguredCustomerCount);
		SetActiveCompanyCount(iActiveCustomerCount);
	}

	// Delayed initialization of the configured number of companies.
	// Made available for situations when the configured number of customers
	// is not known at construction time.
	//
	void SetConfiguredCompanyCount(TIdent iConfiguredCustomerCount)
	{
		m_iConfiguredCompanyCount = CalculateCompanyCount(iConfiguredCustomerCount);
	}

	// Delayed initialization of the active number of companies.
	// Made available for situations when the active number of customers
	// is not known at construction time.
	//
	void SetActiveCompanyCount(TIdent iActiveCustomerCount)
	{
		m_iActiveCompanyCount = CalculateCompanyCount(iActiveCustomerCount);
	}

	// Calculate company count for the specified number of customers.
	// Sort of a static method. Used in parallel generation of company related tables.
	//
	TIdent CalculateCompanyCount(TIdent iCustomerCount)
	{
		return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
	}

	// Calculate the first company id (0-based) for the specified customer id
	//
	TIdent CalculateStartFromCompany(TIdent iStartFromCustomer)
	{
		return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
	}

	// Create company name with appended suffix based on the 
	// load unit number.
	//
	void CreateName(TIdent	iIndex,		// row number
					char*	szOutput,	// output buffer
					int		iOutputLen) // size of the output buffer
	{
		TIdent		iFileIndex = iIndex % CFlatFile<TCompanyInputRow, TCompanyLimits>::GetSize();
		TIdent		iAdd = iIndex / CFlatFile<TCompanyInputRow, TCompanyLimits>::GetSize();

		if (iAdd > 0)
		{
			sprintf( szOutput, "%s #%" PRId64, GetRecord(iFileIndex)->CO_NAME, iAdd );
		}
		else
		{
			strncpy(szOutput, GetRecord(iFileIndex)->CO_NAME, iOutputLen);
		}
	}

	// Return company id for the specified row.
	// Index can exceed the size of the Company input file.
	//
	TIdent GetCompanyId(TIdent iIndex)
	{
		// Index wraps around every 5000 companies.
		//
		return m_list[ (int)(iIndex % m_list.size()) ].CO_ID 
				+ iIndex / m_list.size() * m_list.size();
	}

	// Return the number of companies in the database for
	// a certain number of customers.
	//	
	TIdent GetSize()
	{
		return m_iConfiguredCompanyCount;
	}

	// Return the number of companies in the database for
	// the configured number of customers.
	//	
	TIdent GetConfiguredCompanyCount()
	{
		return m_iConfiguredCompanyCount;
	}

	// Return the number of companies in the database for
	// the active number of customers.
	//	
	TIdent GetActiveCompanyCount()
	{
		return m_iActiveCompanyCount;
	}

	// Overload GetRecord to wrap around indices that 
	// are larger than the flat file
	//
	TCompanyInputRow*	GetRecord(TIdent index) { return &m_list[(int)(index % m_list.size())]; };
};

}	// namespace TPCE

#endif // COMPANY_FILE_H

