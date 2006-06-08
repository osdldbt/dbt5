/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Class representing the Taxrate table.
*/
#ifndef TAXRATE_TABLE_H
#define TAXRATE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CTaxrateTable : public TableTemplate<TAXRATE_ROW>
{
	TTaxRatesCountryFile	*m_pCountry;	// in-mem representation of Country Tax Rate file
	TTaxRatesDivisionFile	*m_pDivision;	// in-mem representation of Division Tax Rate file
	UINT					m_iCountryIndex;	// Country file record index used for generation of the current table record
	UINT					m_iDivisionIndex;	// Division file record index used for generation of the current table record
	UINT					m_iCountrySubIndex;	// subindex in the vector (record) from Country
	UINT					m_iDivisionSubIndex;// subindex in the vector (record) from Division

public:
	CTaxrateTable( CInputFiles inputFiles )
		: TableTemplate<TAXRATE_ROW>(),
		m_pCountry(inputFiles.TaxRatesCountry),
		m_pDivision(inputFiles.TaxRatesDivision),
		m_iCountryIndex(0),
		m_iDivisionIndex(0),
		m_iCountrySubIndex(0),
		m_iDivisionSubIndex(0)
	{
	};	

	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		const vector<TTaxRateInputRow>	*pRatesRecord;

		++m_iLastRowNumber;	//increment state info

		// Return a Country row if not all have been returned
		if (m_iCountryIndex < m_pCountry->GetSize())
		{			
			pRatesRecord = m_pCountry->GetRecord(m_iCountryIndex);	// get into a separate variable for convenience

			// Copy all the necessary fields from the tax rate row of the input file
			strncpy(m_row.TX_ID, (*pRatesRecord)[m_iCountrySubIndex].TAX_ID, sizeof(m_row.TX_ID));
			strncpy(m_row.TX_NAME, (*pRatesRecord)[m_iCountrySubIndex].TAX_NAME, sizeof(m_row.TX_NAME));
			m_row.TX_RATE = (*pRatesRecord)[m_iCountrySubIndex].TAX_RATE;

			++m_iCountrySubIndex;	// move to the next element in the record

			if (m_iCountrySubIndex >= m_pCountry->GetRecord(m_iCountryIndex)->size())
			{	// All elements of the current record have been traversed
				// Advance to the next record in the file
				++m_iCountryIndex;
				m_iCountrySubIndex = 0;	// start with the first element in the record's vector
			}
		}
		else
		{	// Must try to return a Division record
			if (m_iDivisionIndex < m_pDivision->GetSize())
			{			
				pRatesRecord = m_pDivision->GetRecord(m_iDivisionIndex);	// get into a separate variable for convenience

				// Copy all the necessary fields from the tax rate row of the input file
				strncpy(m_row.TX_ID, (*pRatesRecord)[m_iDivisionSubIndex].TAX_ID, sizeof(m_row.TX_ID));
				strncpy(m_row.TX_NAME, (*pRatesRecord)[m_iDivisionSubIndex].TAX_NAME, sizeof(m_row.TX_NAME));
				m_row.TX_RATE = (*pRatesRecord)[m_iDivisionSubIndex].TAX_RATE;

				++m_iDivisionSubIndex;	// move to the next element in the record

				if (m_iDivisionSubIndex >= m_pDivision->GetRecord(m_iDivisionIndex)->size())
				{	// All elements of the current record have been traversed
					// Advance to the next record in the file
					++m_iDivisionIndex;
					m_iDivisionSubIndex = 0;	// start with the first element in the record's vector
				}
			}
		}
		
		// Done when all the records from both Country and Division input files have been returned
		m_bMoreRecords = (m_iCountryIndex < m_pCountry->GetSize()) || (m_iDivisionIndex < m_pDivision->GetSize());

		return ( MoreRecords() );
	}
};

}	// namespace TPCE

#endif //TAXRATE_TABLE_H
