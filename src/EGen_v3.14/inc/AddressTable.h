/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

/*
*	Contains class definition to generate Address table.
*	Address table contains addresses for exchanges, companies, and customers.
*/
#ifndef ADDRESS_TABLE_H
#define ADDRESS_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CAddressTable : public TableTemplate<ADDRESS_ROW>
{
private:
	CPerson					m_person;
	CDateTime				m_date_time;
	CCompanyFile*			m_companies;
	TStreetNamesFile*		m_Street;	
	TStreetSuffixFile*		m_StreetSuffix;
	TZipCodeFile*			m_ZipCode;
	TIdent					m_iStartFromCustomer;
	TIdent					m_iCustomerCount;	//total # of customers for whom to generate addresses
	bool					m_bCustomerAddressesOnly;	// whether generating only customer addresses
	bool					m_bCustomerAddress;	//whether the currently generated row is for a customer
	TIdent					m_iCompanyCount;	//total # of companies for which to generate addresses
	UINT					m_iExchangeCount;	//total # of exchanges for which to generate addresses
	TIdent					m_iTotalAddressCount;	// total # of address rows to generate	

	void GenerateAD_LINE_1();
	void GenerateAD_LINE_2();
	int	GetTownDivisionZipCodeThreshold(TIdent ADID);
	int GetCountryCode( const char *pZipCode );
	void GenerateAD_ZC_CODE_CTRY();

	/*
	*	Reset the state for the next load unit
	*/
	void InitNextLoadUnit();

public:
	/*
	*  Constructor for the ADDRESS table class.
	*
	*  PARAMETERS:
	*		IN  inputFiles				- input flat files loaded in memory
	*		IN  iCustomerCount			- number of customers to generate
	*		IN  iStartFromCustomer		- ordinal position of the first customer in the sequence (Note: 1-based)
	*									  for whom to generate the addresses. 
	*									  Used if generating customer addresses only.
	*		IN	bCustomerAddressesOnly	- if true, generate only customer addresses
	*									  if false, generate exchange, company, and customer addresses 
	*									   (always start from the first customer in this case)
	*/
	CAddressTable(CInputFiles	inputFiles, 
				TIdent			iCustomerCount,
				TIdent			iStartFromCustomer,
				bool			bCustomerAddressesOnly = false);

	// Generate and return next address id.
	//
	TIdent GenerateNextAD_ID();

	TIdent GetAD_IDForCustomer(TIdent C_ID);	// return address id for the customer id

	// Return division and country code for address id AD_ID.
	// Used in the loader to properly calculate tax on a trade.
	void GetDivisionAndCountryCodesForAddress(TIdent AD_ID, int &iDivCode, int &iCtryCode);

	// Return division and country codes for current address.
	// Used in generating customer taxrates.
	void GetDivisionAndCountryCodes(int &iDivCode, int &iCtryCode)
	{
		GetDivisionAndCountryCodesForAddress(m_row.AD_ID, iDivCode, iCtryCode);
	}

	bool GenerateNextRecord();	//generates the next table row

};

}	// namespace TPCE

#endif //ADDRESS_TABLE_H
