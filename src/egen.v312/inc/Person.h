/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Person class for the Customer table.
*/
#ifndef PERSON_H
#define PERSON_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

// Used for generating tax ID strings.
const int	TaxIDFmt_len = 14;
const char	TaxIDFmt[TaxIDFmt_len+1] = "nnnaannnnaannn";

class CPerson
{
private:	

	TLastNamesFile			*m_LastNames;
	TMaleFirstNamesFile		*m_MaleFirstNames;
	TFemaleFirstNamesFile	*m_FemaleFirstNames;

	CRandom			m_rnd;	

public:
	CPerson(CInputFiles inputFiles);
	char* GetLastName(TIdent CID);
	char* GetFirstName(TIdent CID);
	char GetMiddleName(TIdent CID);
	char GetGender(TIdent CID);	//'M' or 'F'
	bool IsMaleGender(TIdent CID);	//TRUE if male, FALSE if female	
	void GetTaxID(TIdent CID, char *buf);

	//get first name, last name, and tax id
	void GetFirstLastAndTaxID(TIdent C_ID, char *szFirstName, char *szLastName, char *szTaxID);

};

}	// namespace TPCE

#endif //PERSON_H
