/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

#include "../inc/EGenTables_stdafx.h"

using namespace TPCE;

// Percentages used in determining gender.
const int	iPercentGenderIsMale = 49;

/*
*	Initializes in-memory representation of names files.
*/
CPerson::CPerson(CInputFiles inputFiles)
	: m_LastNames(inputFiles.LastNames), 
	m_MaleFirstNames(inputFiles.MaleFirstNames),
	m_FemaleFirstNames(inputFiles.FemaleFirstNames)
{	
}


/*
*	Returns the last name for a particular customer id. 
*	It'll always be the same for the same customer id.
*/
char* CPerson::GetLastName(TIdent CID)
{
	RNGSEED	OldSeed;
	int		iThreshold;

	OldSeed = m_rnd.GetSeed();

	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseLastName, CID ));

	//generate Threshold up to the value of the last key (first member in a pair)
	iThreshold = m_rnd.RndIntRange(0, m_LastNames->GetGreatestKey() - 1);

	m_rnd.SetSeed( OldSeed );

	char *t;
	t = (m_LastNames->GetRecord(iThreshold))->LAST_NAME;
	return t;
}

/*
*	Returns the first name for a particular customer id.
*	Determines gender first.
*/
char* CPerson::GetFirstName(TIdent CID)
{
	RNGSEED	OldSeed;
	int		iThreshold;
	char	*FirstName;

	OldSeed = m_rnd.GetSeed();
	
	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseFirstName, CID ));

	//Find out gender
	if (IsMaleGender(CID))
	{
		iThreshold = m_rnd.RndIntRange(0, m_MaleFirstNames->GetGreatestKey() - 1);
		FirstName = (m_MaleFirstNames->GetRecord(iThreshold))->FIRST_NAME;
	}
	else
	{
		iThreshold = m_rnd.RndIntRange(0, m_FemaleFirstNames->GetGreatestKey() - 1);
		FirstName = (m_FemaleFirstNames->GetRecord(iThreshold))->FIRST_NAME;
	}
	m_rnd.SetSeed( OldSeed );
	return( FirstName );	
}
/*
*	Returns the middle name.
*/
char CPerson::GetMiddleName(TIdent CID)
{
	RNGSEED	OldSeed;
	char	cMiddleInitial[2];

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseMiddleInitial, CID ));
	cMiddleInitial[1] = '\0';
	m_rnd.RndAlphaNumFormatted( cMiddleInitial, (char *) "a" );
	m_rnd.SetSeed( OldSeed );
	return( cMiddleInitial[0] );
}

/*
*	Returns the gender character for a particular customer id.
*/
char CPerson::GetGender(TIdent CID)
{
	RNGSEED	OldSeed;
	char	cGender;

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseGender, CID ));

	//Find out gender
	if (m_rnd.RndPercent( iPercentGenderIsMale ))
	{
		cGender = 'M';
	}
	else
	{
		cGender = 'F';
	}

	m_rnd.SetSeed( OldSeed );
	return( cGender );
}

/*
*	Returns TRUE is a customer id is male
*/
bool CPerson::IsMaleGender(TIdent CID)
{
	return GetGender(CID)=='M';
}

/*
*	Generate tax id
*/
void CPerson::GetTaxID(TIdent CID, char *buf)
{
	RNGSEED	OldSeed;

	OldSeed = m_rnd.GetSeed();

	// NOTE: the call to RndAlphaNumFormatted "consumes" an RNG value
	// for EACH character in the format string. Therefore, to avoid getting
	// tax ID's that overlap N-1 out of N characters, multiply the offset into
	// the sequence by N to get a unique range of values.
	m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseTaxID, ( CID * TaxIDFmt_len )));
	m_rnd.RndAlphaNumFormatted(buf, (char *)TaxIDFmt);
	m_rnd.SetSeed( OldSeed );
}

/*
*	Get first name, last name, and tax id.
*/
void CPerson::GetFirstLastAndTaxID(TIdent C_ID, char *szFirstName, char *szLastName, char *szTaxID)
{
	//Fill in the last name
	strncpy(szLastName, GetLastName(C_ID), cL_NAME_len);	
	//Fill in the first name
	strncpy(szFirstName, GetFirstName(C_ID), cF_NAME_len);	
	//Fill in the tax id
	GetTaxID(C_ID, szTaxID);
}


