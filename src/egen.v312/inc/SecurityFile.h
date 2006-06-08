/******************************************************************************
*	(c) Copyright 2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		Doug Johnson (HP)
*
*	Description:		Implementation of the Security input file that scales
*						with the database size.
*
******************************************************************************/

#ifndef SECURITY_FILE_H
#define SECURITY_FILE_H

#include "CompanyFile.h"

namespace TPCE
{

// We use a small set of values for 26 raised to a power, so store them in
// a constant array to save doing calls to pow( 26.0, ? )
static const int	Power26[] = { 1, 26, 676, 17576, 456976, 11881376, 308915776 };

// For index i > 0, this array holds the sum of 26^0 ... 26^(i-1)
static const INT64	Power26Sum[] = { 0, 1, 27, 703, 18279, 475255, 12356631, 321272407, 8353082583ULL };

class CSecurityFile	: public CFlatFile<TSecurityInputRow, TSecurityLimits>
{
	// Total number of securities in the database.
	// Depends on the total number of customers.
	//
	TIdent	m_iConfiguredSecurityCount;
	TIdent	m_iActiveSecurityCount;

	// Number of base companies (=rows in Company.txt input file).
	//
	UINT	m_iBaseCompanyCount;

	// Used to map a symbol to it's id value
	bool				m_SymbolToIdMapIsLoaded;
	map< string, int >	m_SymbolToIdMap;
	map< char, int >	m_LowerCaseLetterToIntMap;
	char				m_SUFFIX_SEPARATOR;

	void CreateSuffix( TIdent Multiplier, char* pBuf, int BufSize )
	{
		int CharCount( 0 );
		INT64 Offset( 0 );
		INT64 LCLIndex( 0 );	// LowerCaseLetter array index

		while( Multiplier - Power26Sum[CharCount+1] >= 0 )
		{
			CharCount++;
		}

		if( CharCount + 2 <= BufSize )	// 1 extra for separator and 1 extra for terminating NULL
		{
			*pBuf = m_SUFFIX_SEPARATOR;
			pBuf++;
			// CharCount is the number of letters needed in the suffix
			// The base string is a string of 'a's of length CharCount
			// Find the offset from the base value represented by the string
			// of 'a's to the desired number, and modify the base string
			// accordingly.
			Offset = Multiplier - Power26Sum[CharCount];

			while( CharCount > 0 )
			{
				LCLIndex = Offset / Power26[ CharCount-1 ];
				*pBuf = LowerCaseLetters[ LCLIndex ];
				pBuf++;
				Offset -= ( LCLIndex * Power26[ CharCount-1 ] );
				CharCount--;
			}
			*pBuf = '\0';
		}
		else
		{
			// Not enough room in the buffer
			CharCount = BufSize - 1;
			while( CharCount > 0 )
			{
				*pBuf = m_SUFFIX_SEPARATOR;
				pBuf++;
				CharCount--;
			}
			*pBuf = '\0';
		}
	}

	INT64 ParseSuffix( const char* pSymbol )
	{
		int CharCount( 0 );
		INT64 Multiplier( 0 );

		CharCount = (int) strlen( pSymbol );

		Multiplier = Power26Sum[CharCount];

		while( CharCount > 0 )
		{
			Multiplier += Power26[ CharCount-1 ] * m_LowerCaseLetterToIntMap[ *pSymbol ];
			CharCount--;
			pSymbol++;
		}
		return( Multiplier );
	}

public:	

	//// Constructor.
	////
	//CSecurityFile(const char *szListFile)
	//: CFlatFile<TSecurityInputRow, TSecurityLimits>(szListFile)	
	//, m_iTotalSecurityCount(0)	// must be initialized later
	//, m_SymbolToIdMapIsLoaded( false )
	//{
	//	TCompanyLimits		CompanyInputFileLimits;
	//
	//	m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	//}
	//
	//// Constructor.
	////
	//CSecurityFile(const string &str)
	//: CFlatFile<TSecurityInputRow, TSecurityLimits>(str)
	//, m_iTotalSecurityCount(0)	// must be initialized later
	//, m_SymbolToIdMapIsLoaded( false )
	//{
	//	TCompanyLimits		CompanyInputFileLimits;
	//	
	//	m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	//}

	// Constructor.
	//
	CSecurityFile(const char *szListFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TSecurityInputRow, TSecurityLimits>(szListFile)	
	, m_SymbolToIdMapIsLoaded( false )
	{	
		TCompanyLimits		CompanyInputFileLimits;

		m_SUFFIX_SEPARATOR = '-';
		SetConfiguredSecurityCount(iConfiguredCustomerCount);
		SetActiveSecurityCount(iActiveCustomerCount);

		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}
	
	// Constructor.
	//
	CSecurityFile(const string &str, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
	: CFlatFile<TSecurityInputRow, TSecurityLimits>(str)	
	, m_SymbolToIdMapIsLoaded( false )
	{	
		TCompanyLimits		CompanyInputFileLimits;

		m_SUFFIX_SEPARATOR = '-';
		SetConfiguredSecurityCount(iConfiguredCustomerCount);
		SetActiveSecurityCount(iActiveCustomerCount);

		m_iBaseCompanyCount		= CompanyInputFileLimits.m_iTotalElements;
	}
	
	// Delayed initialization of the configured number of securities.
	// Made available for situations when the configured number of customers
	// is not known at construction time.
	//
	void SetConfiguredSecurityCount(TIdent iConfiguredCustomerCount)
	{
		m_iConfiguredSecurityCount = CalculateSecurityCount(iConfiguredCustomerCount);
	}

	// Delayed initialization of the active number of securities.
	// Made available for situations when the active number of customers
	// is not known at construction time.
	//
	void SetActiveSecurityCount(TIdent iActiveCustomerCount)
	{
		m_iActiveSecurityCount = CalculateSecurityCount(iActiveCustomerCount);
	}

	// Calculate total security count for the specified number of customers.
	// Sort of a static method. Used in parallel generation of securities related tables.
	//
	TIdent CalculateSecurityCount(TIdent iCustomerCount)
	{
		return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitSecurityCount;
	}

	// Calculate the first security id (0-based) for the specified customer id
	//
	TIdent CalculateStartFromSecurity(TIdent iStartFromCustomer)
	{
		return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitSecurityCount;
	}

	// Create security symbol with mod/div magic.
	//
	// This function is needed to scale unique security
	// symbols with the database size.
	//
	void CreateSymbol(	TIdent	iIndex,		// row number
						char*	szOutput,	// output buffer
						int		iOutputLen)	// size of the output buffer (including null)
	{
		TIdent	iFileIndex	= iIndex % CFlatFile<TSecurityInputRow, TSecurityLimits>::GetSize();
		TIdent	iAdd		= iIndex / CFlatFile<TSecurityInputRow, TSecurityLimits>::GetSize();
		int		iNewLen;

		// Load the base symbol
		strncpy(szOutput, GetRecord(iFileIndex)->S_SYMB, iOutputLen);

		szOutput[iOutputLen - 1] = '\0';	// Ensure NULL termination

		// Add a suffix if needed
		if (iAdd > 0)
		{
			iNewLen = (int) strlen( szOutput );
			CreateSuffix( iAdd, &szOutput[iNewLen], iOutputLen - iNewLen );
		}		
	}

	// Return company id for the specified row of the SECURITY table.
	// Index can exceed the size of the Security flat file.
	//
	TIdent GetCompanyId(TIdent iIndex)
	{
		// Index wraps around every 6850 securities (5000 companies).
		//
		return m_list[ (int)(iIndex % m_list.size()) ].S_CO_ID 
				+ iIndex / m_list.size() * m_iBaseCompanyCount;
	}

	TIdent GetCompanyIndex( TIdent Index )
	{
		// Indices and Id's are offset by 1
		return( GetCompanyId( Index ) - 1 );
	}

	// Return the number of securities in the database for
	// a certain number of customers.
	//	
	TIdent GetSize()
	{
		return m_iConfiguredSecurityCount;
	}

	// Return the number of securities in the database for
	// the configured number of customers.
	//	
	TIdent GetConfiguredSecurityCount()
	{
		return m_iConfiguredSecurityCount;
	}

	// Return the number of securities in the database for
	// the active number of customers.
	//	
	TIdent GetActiveSecurityCount()
	{
		return m_iActiveSecurityCount;
	}

	// Overload GetRecord to wrap around indices that 
	// are larger than the flat file
	//
	TSecurityInputRow*	GetRecord(TIdent index) { return &m_list[(int)(index % m_list.size())]; };

	// Load the symbol-to-id map
	bool LoadSymbolToIdMap( void )
	{
		if( ! m_SymbolToIdMapIsLoaded )
		{
			int ii;

			for( ii = 0; ii < m_list.size(); ii++ )
			{
				m_SymbolToIdMap[ m_list[ii].S_SYMB ] = m_list[ii].S_ID;
			}
			m_SymbolToIdMapIsLoaded = true;

			for( ii = 0; ii < MaxLowerCaseLetters; ii++ )
			{
				m_LowerCaseLetterToIntMap[ LowerCaseLetters[ii] ] = ii;
			}
		}
		return( m_SymbolToIdMapIsLoaded );
	}

	TIdent GetId( char* pSymbol )
	{
		char*	pSeparator( NULL );

		if( !m_SymbolToIdMapIsLoaded )
		{
			LoadSymbolToIdMap();
		}
		if( NULL == ( pSeparator = strchr( pSymbol, m_SUFFIX_SEPARATOR )))
		{
			// we're dealing with a base symbol
			return( m_SymbolToIdMap[ pSymbol ] );
		}
		else
		{
			// we're dealing with an extended symbol
			char*	pSuffix( NULL );
			TIdent	BaseId( 0 );
			TIdent	Multiplier( 0 );

			*pSeparator = '\0';						// Temporarily split the symbol into base and suffix parts
			BaseId = m_SymbolToIdMap[ pSymbol ];

			pSuffix = pSeparator + 1;				// The suffix starts right after the separator character
			Multiplier = (int)ParseSuffix( pSuffix );// For now, suffix values fit in an int, cast ParseSuffix to avoid compiler warning

			*pSeparator = m_SUFFIX_SEPARATOR;			// Re-connect the base symbol and the suffix
			return(( Multiplier * m_list.size() ) + BaseId );
		}
	}

	TIdent GetIndex( char* pSymbol )
	{
		// Indices and Id's are offset by 1
		return( GetId( pSymbol ) - 1 );
	}
};

}	// namespace TPCE

#endif // SECURITY_FILE_H

