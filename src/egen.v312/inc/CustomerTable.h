/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Contains class definition to generate Customer table.
*/
#ifndef CUSTOMER_TABLE_H
#define CUSTOMER_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{
const int		iNumEMAIL_DOMAINs = 6;
static char		EMAIL_DOMAINs[iNumEMAIL_DOMAINs][15] = {"@msn.com","@hotmail.com","@rr.com","@netzero.com","@earthlink.com","@attbi.com"};

class CCustomerTable : public TableTemplate<CUSTOMER_ROW>
{
private:	
	TIdent				m_iRowsToGenerate;	//total # of rows to generate
	CPerson				m_person;
	CDateTime			m_date_time;
	TAreaCodeFile*		m_Phones;
	TIdent				m_iCompanyCount;	// number of Companies
	unsigned int		m_iExchangeCount;	// number of Exchanges
	TStatusTypeFile*	m_StatusTypeFile;	// STATUS_TYPE table from the flat file
	CCustomerSelection	m_CustomerSelection;

	void GenerateC_ST_ID();
	void GeneratePersonInfo();	//generate last name, first name, and gender.	
	void GenerateC_DOB();
	void GenerateC_AD_ID();
	void GenerateC_CTRY_1();
	void GenerateC_LOCAL_1();
	void GenerateC_AREA_1();
	void GenerateC_EXT_1();
	void GenerateC_CTRY_2();
	void GenerateC_LOCAL_2();
	void GenerateC_AREA_2();
	void GenerateC_EXT_2();	
	void GenerateC_CTRY_3();
	void GenerateC_LOCAL_3();
	void GenerateC_AREA_3();
	void GenerateC_EXT_3();	
	void GenerateC_EMAIL_1_and_C_EMAIL_2();	

	/*
	*	Reset the state for the next load unit
	*/
	void InitNextLoadUnit();

public:
	/*
	*  Constructor for the CUSTOMER table class.
	*
	*  PARAMETERS:
	*		IN  inputFiles			- input flat files loaded in memory
	*		IN  iCustomerCount		- number of customers to generate
	*		IN  iStartFromCustomer	- ordinal position of the first customer in the sequence (Note: 1-based)
	*/
	CCustomerTable(CInputFiles inputFiles, TIdent iCustomerCount, TIdent iStartFromCustomer);

	TIdent GenerateNextC_ID();	//generate C_ID and store state information; return false if all ids are generated
	TIdent GetCurrentC_ID() {return m_row.C_ID;}	//return current customer id
	
	void GetC_TAX_ID(TIdent C_ID, char *szOutput);	//return tax id (ala Social Security number)
	eCustomerTier GetC_TIER(TIdent C_ID);	//returns unique C_TIER for a given customer id

	bool GenerateNextRecord();	//generates the next table row
	
};

}	// namespace TPCE

#endif //CUSTOMER_TABLE_H
