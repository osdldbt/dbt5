/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


#include "../inc/EGenTables_stdafx.h"

using namespace TPCE;

const int iUSACtryCode = 1;		//must be the same as the code in country tax rates file
const int iCanadaCtryCode = 2;	//must be the same as the code in country tax rates file

// Minimum and maximum to use when generating address street numbers.
const int iStreetNumberMin = 100;
const int iStreetNumberMax = 25000;

//Some customers have an AD_LINE_2, some are NULL.
const int iPctCustomersWithNullAD_LINE_2 = 60;

// Of the customers that have an AD_LINE_2, some are
// an apartment, others are a suite.
const int iPctCustomersWithAptAD_LINE_2 = 75;

// Minimum and maximum to use when generating apartment numbers.
const int iApartmentNumberMin = 1;
const int iApartmentNumberMax = 1000;

// Minimum and maximum to use when generating suite numbers.
const int iSuiteNumberMin = 1;
const int iSuiteNumberMax = 500;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const int iRNGSkipOneRowAddress = 10;	// real number in 3.5: 7

/*
*	CustomerTable constructor
*/
CAddressTable::CAddressTable(CInputFiles	inputFiles, 
							 TIdent			iCustomerCount, 
							 TIdent			iStartFromCustomer,
							 bool			bCustomerAddressesOnly)
: TableTemplate<ADDRESS_ROW>()
, m_person(inputFiles)
, m_companies(inputFiles.Company)
, m_Street(inputFiles.Street)
, m_StreetSuffix(inputFiles.StreetSuffix)	
, m_ZipCode(inputFiles.ZipCode)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iCustomerCount(iCustomerCount)
, m_bCustomerAddressesOnly(bCustomerAddressesOnly)
, m_bCustomerAddress(bCustomerAddressesOnly)
{	
	m_iExchangeCount = (UINT)inputFiles.Exchange->GetSize();	// number of rows in Exchange
	m_iCompanyCount = m_companies->GetConfiguredCompanyCount();	// number of configured companies
	
	// Generate customer addresses only (used for CUSTOMER_TAXRATE)
	if (bCustomerAddressesOnly)
	{
		// skip exchanges and companies
		m_row.AD_ID = m_iExchangeCount + m_iCompanyCount + iStartFromCustomer - 1;

		// advance the random number generator
		m_rnd.SetSeed(m_rnd.RndNthElement(m_rnd.GetSeed(), 
										  m_iExchangeCount + m_iCompanyCount + iStartFromCustomer - 1));

		m_iTotalAddressCount = m_iCustomerCount;
	}
	else
	{	// Generating not only customer, but also exchange and company addresses
		m_iTotalAddressCount = m_iCustomerCount + m_iCompanyCount + m_iExchangeCount;
	}
}

/*
*	Reset the state for the next load unit
*/
void CAddressTable::InitNextLoadUnit()
{
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, 
									 m_row.AD_ID * iRNGSkipOneRowAddress));
}

/*
*	Generates only the next A_ID value
*/
TIdent CAddressTable::GenerateNextAD_ID()
{
	if (   m_row.AD_ID > (m_iExchangeCount + m_iCompanyCount)
		&& ( (m_row.AD_ID - (m_iExchangeCount + m_iCompanyCount) ) % iDefaultLoadUnitSize == 0))
	{
		InitNextLoadUnit();
	}

	++m_iLastRowNumber;
	// Find out whether this next row is for a customer (so as to generate AD_LINE_2).
	// Exchange and Company addresses are before Customer ones.
	if (!m_bCustomerAddressesOnly)
	{
		m_bCustomerAddress = m_iLastRowNumber >= m_iExchangeCount + m_iCompanyCount;
	}
	//update state info
	m_bMoreRecords = m_iLastRowNumber < m_iTotalAddressCount;
	return ++m_row.AD_ID;	//sequential for now.
}

/*
*	Returns the address id of the customer specified by the customer id.
*/
TIdent CAddressTable::GetAD_IDForCustomer(TIdent C_ID)
{
	return m_iExchangeCount + m_iCompanyCount + C_ID;
}

/*
*	Generate AD_LINE_1.
*/
void CAddressTable::GenerateAD_LINE_1()
{
	int iStreetNum = m_rnd.RndIntRange(iStreetNumberMin, iStreetNumberMax);
	int iStreetThreshold = m_rnd.RndIntRange(0, m_Street->GetGreatestKey()-2);
	int iStreetSuffixThreshold = m_rnd.RndIntRange(0, m_StreetSuffix->GetGreatestKey()-1);	

	sprintf(m_row.AD_LINE1, "%d %s %s", 
			iStreetNum, 
			(m_Street->GetRecord(iStreetThreshold))->STREET,
			(m_StreetSuffix->GetRecord(iStreetSuffixThreshold))->SUFFIX);
}

/*
*	Generate AD_LINE_@.
*/
void CAddressTable::GenerateAD_LINE_2()
{
	if (!m_bCustomerAddress || m_rnd.RndPercent(iPctCustomersWithNullAD_LINE_2))
	{	//Generate second address line only for customers (not companies)
		m_row.AD_LINE2[0] = '\0';
	}
	else
	{
		if (m_rnd.RndPercent(iPctCustomersWithAptAD_LINE_2))
		{
			sprintf(m_row.AD_LINE2, "Apt. %d", m_rnd.RndIntRange(iApartmentNumberMin, iApartmentNumberMax));
		}
		else
		{
			sprintf(m_row.AD_LINE2, "Suite %d", m_rnd.RndIntRange(iSuiteNumberMin, iSuiteNumberMax));
		}
	}	
}

/*
*	For a given address id returns the same Threshold used to 
*	select the town, division, zip, and country.
*	Needed to return a specific division/country for a given address id (for customer tax rates).
*/
int	CAddressTable::GetTownDivisionZipCodeThreshold(TIdent ADID)
{
	RNGSEED OldSeed;
	int		iThreshold;

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseTownDivZip, ADID ));
	iThreshold = m_rnd.RndIntRange(0, m_ZipCode->GetGreatestKey()-1);
	m_rnd.SetSeed( OldSeed );
	return( iThreshold );
}
/*
*	Return the country code code for a given zip code.
*/
int CAddressTable::GetCountryCode( const char *szZipCode )
{
	if(( '0' <= szZipCode[0] )&&( szZipCode[0] <= '9' ))
	{
		// If the zip code starts with a number, then it's a USA code.
		return( iUSACtryCode );
	}
	else
	{
		// If the zip code does NOT start with a number, than it's a Canadian code.
		return( iCanadaCtryCode );
	}
}
/*
*	Returns a certain division/country code (from the input file) for a given address id.
*/
void CAddressTable::GetDivisionAndCountryCodesForAddress(TIdent AD_ID, int &iDivCode, int &iCtryCode)
{
	int iThreshold = GetTownDivisionZipCodeThreshold(AD_ID);
	// Select the row in the input file
	const TZipCodeInputRow* pZipCodeInputRow = m_ZipCode->GetRecord(iThreshold);
	iDivCode = pZipCodeInputRow->iDivisionTaxKey;	//return division code
	// Return country code
	iCtryCode = GetCountryCode( pZipCodeInputRow->ZC_CODE );
}

/*
*	Generate town
*/
void CAddressTable::GenerateAD_ZC_CODE_CTRY()
{
	int iThreshold;
	const TZipCodeInputRow*	pZipCodeInputRow;

	iThreshold = GetTownDivisionZipCodeThreshold(m_row.AD_ID);

	pZipCodeInputRow = m_ZipCode->GetRecord(iThreshold);

	strncpy( m_row.AD_ZC_CODE, pZipCodeInputRow->ZC_CODE, sizeof( m_row.AD_ZC_CODE ) - 1 );

	if( iUSACtryCode == GetCountryCode( pZipCodeInputRow->ZC_CODE ))
	{	//US state
		strncpy(m_row.AD_CTRY, "USA", sizeof(m_row.AD_CTRY)-1);
	}
	else
	{	//Canadian province
		strncpy(m_row.AD_CTRY, "CANADA", sizeof(m_row.AD_CTRY)-1);
	}	
}

/*
*	Generates all column values for the next row
*/
bool CAddressTable::GenerateNextRecord()
{	
	GenerateNextAD_ID();
	GenerateAD_LINE_1();
	GenerateAD_LINE_2();
	GenerateAD_ZC_CODE_CTRY();
	
	//Return false if all the rows have been generated
	return (MoreRecords());
}
