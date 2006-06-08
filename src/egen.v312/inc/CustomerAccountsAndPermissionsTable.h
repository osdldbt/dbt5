/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing the Customer Accounts table.
*/
#ifndef CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H
#define CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{
const int		iMaxCAPerms=3;	//maximum # of customers having permissions to the same account
const int		iMinAccountsPerCustRange[3] = {1, 2, 5};
const int		iMaxAccountsPerCustRange[3] = {4, 8, 10};
const int		iMaxAccountsPerCust = 10;	// must be the biggest number in iMaxAccountsPerCustRange array
const int		iStartingBrokerID = 1;

//	This is the fixed range from which person ids (like CIDs) are selected
//	for the *additional* permissions on the account that make the
//	content of ACCOUNT_PERMISSION table.
//
//	The range is fixed for any size database in order for the parallel loaders
//	to select person ids the same way and be compatible with runtime driver
//	(and to avoid database size parameter to the loader executable).
//
const int		iAccountPermissionIDRange = 1024 * 1024 * 1024 - iDefaultStartFromCustomer;

const int		iPercentAccountsWithPositiveInitialBalance = 80;

const double	fAccountInitialPositiveBalanceMax = 9999999.99;
const double	fAccountInitialNegativeBalanceMin = -9999999.99;

const int		iPercentAccountAdditionalPermissions_0 = 60;
const int		iPercentAccountAdditionalPermissions_1 = 38;
const int		iPercentAccountAdditionalPermissions_2 = 2;

const int		iPercentAccountTaxStatusNonTaxable = 20;
const int		iPercentAccountTaxStatusTaxableAndWithhold = 50;
const int		iPercentAccountTaxStatusTaxableAndDontWithhold = 30;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const int iRNGSkipOneRowCustomerAccount = 10;	// real max count in v3.5: 7

enum eTaxStatus
{
	eNonTaxable = 0,
	eTaxableAndWithhold,
	eTaxableAndDontWithhold
};

typedef struct CUSTOMER_ACCOUNT_AND_PERMISSION_ROW
{
	CUSTOMER_ACCOUNT_ROW	m_ca;
	ACCOUNT_PERMISSION_ROW	m_perm[iMaxCAPerms+1];
} *PCUSTOMER_ACCOUNT_AND_PERMISSION_ROW;

class CCustomerAccountsAndPermissionsTable : public TableTemplate<CUSTOMER_ACCOUNT_AND_PERMISSION_ROW>
{
	TTaxableAccountNameFile*	m_pTaxableAccountName;
	TNonTaxableAccountNameFile*	m_pNonTaxableAccountName;
	TIdent						m_iStartFromCustomer;
	TIdent						m_iCustomerCount;
	UINT						m_iRowsToGenForCust;	//total # of rows to generate for a given portfolio
	UINT						m_iRowsGeneratedForCust;	//rows already generated for a particular portfolio	
	CCustomerTable				m_cust;
	CPerson						m_person;
	UINT						m_iPermsForCA;
	TIdent						m_iStartFromBroker;
	TIdent						m_iBrokersCount;
	CAddressTable				m_addr;		//ADDRESS table - to calculate tax for TRADE
	UINT						m_iLoadUnitSize;

	/*
	*	Reset the state for the next load unit
	*/
	void InitNextLoadUnit()
	{
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, 
										  GetCurrentC_ID() * iMaxAccountsPerCust *
										  iRNGSkipOneRowCustomerAccount));
	}

	/*
	*	Generate only the Customer Account row
	*/
	void GenerateCARow()
	{
		int		iAcctType;

		//Generate customer account row.

		GenerateNextCA_AD();
				
		m_row.m_ca.CA_C_ID = GetCurrentC_ID();	//get from CUSTOMER		

		// Generate broker id.		
		m_row.m_ca.CA_B_ID = GenerateBrokerIdForAccount(m_row.m_ca.CA_ID);		
				
		// Generate tax status and account name.
		if ((m_row.m_ca.CA_TAX_ST = GetAccountTaxStatus(m_row.m_ca.CA_ID)) == eNonTaxable)
		{	// non-taxable account
			iAcctType = (int) m_row.m_ca.CA_ID % m_pNonTaxableAccountName->GetSize();	// select account type

			sprintf (m_row.m_ca.CA_NAME, 
					"%s %s %s", 
					m_person.GetFirstName(m_row.m_ca.CA_C_ID), 
					m_person.GetLastName(m_row.m_ca.CA_C_ID),
					(m_pNonTaxableAccountName->GetRecord(iAcctType))->NAME);
		}
		else
		{	// taxable account
			iAcctType = (int) m_row.m_ca.CA_ID % m_pTaxableAccountName->GetSize();	// select account type

			sprintf (m_row.m_ca.CA_NAME, 
					"%s %s %s", 
					m_person.GetFirstName(m_row.m_ca.CA_C_ID), 
					m_person.GetLastName(m_row.m_ca.CA_C_ID),
					(m_pTaxableAccountName->GetRecord(iAcctType))->NAME);
		}

		if (m_rnd.RndPercent( iPercentAccountsWithPositiveInitialBalance ))
		{
			m_row.m_ca.CA_BAL = m_rnd.RndDoubleIncrRange(0.00, fAccountInitialPositiveBalanceMax, 0.01);
		}
		else
		{
			m_row.m_ca.CA_BAL = m_rnd.RndDoubleIncrRange(fAccountInitialNegativeBalanceMin, 0.00, 0.01);
		}
	}

	/*
	*	Helper function to generate parts of an ACCOUNT_PERMISSION row
	*/
	void FillAPRow(TIdent CA_ID, TIdent C_ID, char *szACL, ACCOUNT_PERMISSION_ROW &row)
	{		
		row.AP_CA_ID = CA_ID;		
		m_cust.GetC_TAX_ID(C_ID, row.AP_TAX_ID);
		strncpy(row.AP_L_NAME, m_person.GetLastName(C_ID), sizeof(row.AP_L_NAME));
		strncpy(row.AP_F_NAME, m_person.GetFirstName(C_ID), sizeof(row.AP_F_NAME));
		strncpy(row.AP_ACL, szACL, sizeof(row.AP_ACL));
	}

	/*
	*	Generate only the Account Permissions row(s)
	*/
	void GenerateAPRows()
	{
		int		iAdditionalPerms;
		TIdent	CID_1, CID_2;

		//Generate account permissions rows.

		//Generate the owner row		
		FillAPRow(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, (char*)"0000", m_row.m_perm[0]);

		iAdditionalPerms = GetNumPermsForCA(m_row.m_ca.CA_ID);
		switch (iAdditionalPerms)
		{
		case 0: m_iPermsForCA = 1;	//60%
			break;
		case 1:
			GetCIDsForPermissions(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, &CID_1, NULL);
			m_iPermsForCA = 2;	//38%
			//generate second account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_1, (char*)"0001", m_row.m_perm[1]);
			break;
		case 2:
			GetCIDsForPermissions(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, &CID_1, &CID_2);
			m_iPermsForCA = 3;	//2%
			//generate second account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_1, (char*)"0001", m_row.m_perm[1]);
			//generate third account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_2, (char*)"0011", m_row.m_perm[2]);
			break;
		}		
		
	}

public:
	CCustomerAccountsAndPermissionsTable(CInputFiles	inputFiles,
										UINT			iLoadUnitSize,	// # of customers in one load unit
										TIdent			iCustomerCount,
										TIdent			iStartFromCustomer)
		: TableTemplate<CUSTOMER_ACCOUNT_AND_PERMISSION_ROW>()
		, m_pTaxableAccountName(inputFiles.TaxableAccountName)
		, m_pNonTaxableAccountName(inputFiles.NonTaxableAccountName)
		, m_iStartFromCustomer(iStartFromCustomer)
		, m_iCustomerCount(iCustomerCount)
		, m_iRowsToGenForCust(0), m_iRowsGeneratedForCust(0)
		, m_cust(inputFiles, iCustomerCount, iStartFromCustomer)
		, m_person(inputFiles)
		, m_iPermsForCA(0)
		, m_iStartFromBroker(iStartFromCustomer / iBrokersDiv)
		, m_iBrokersCount(iLoadUnitSize / iBrokersDiv)
		, m_addr(inputFiles, iCustomerCount, iStartFromCustomer)
		, m_iLoadUnitSize(iLoadUnitSize)
	{
	};

	/*
	*	Generate the number of accounts for a given customer id.
	*/
	int GetNumberOfAccounts(TIdent CID, eCustomerTier iCustomerTier)
	{
		RNGSEED			OldSeed;
		int				iNumberOfAccounts;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseNumberOfAccounts, CID ));
		iNumberOfAccounts = m_rnd.RndIntRange(	iMinAccountsPerCustRange[iCustomerTier - eCustomerTierOne],
												iMaxAccountsPerCustRange[iCustomerTier - eCustomerTierOne] );
		m_rnd.SetSeed( OldSeed );
		return( iNumberOfAccounts );
	}

	/*
	*	Generate a random account for the specified customer.
	*	The distribution is uniform across all the accounts for the customer.
	*/
	void GenerateRandomAccountId(CRandom&		RND,				// in - external RNG
								TIdent			iCustomerId,		// in
								eCustomerTier	iCustomerTier,		// in
								TIdent*			piCustomerAccount,	// out
								int*			piAccountCount)		// out
	{
		TIdent	iCustomerAccount;
		int		iAccountCount;
		TIdent	iStartingAccount;

		iAccountCount = GetNumberOfAccounts(iCustomerId, iCustomerTier);

		iStartingAccount = GetStartingCA_ID(iCustomerId);

		// Select random account for the customer
		//
		iCustomerAccount = RND.RndInt64Range(iStartingAccount, 
											iStartingAccount + iAccountCount - 1);

		if (piCustomerAccount != NULL)
		{
			*piCustomerAccount = iCustomerAccount;
		}

		if (piAccountCount != NULL)
		{
			*piAccountCount = iAccountCount;
		}
	}

	/*
	*	Generate a non-uniform random account for the specified customer.
	*/
	TIdent GenerateNonUniformRandomAccountId( CRandom& RND, TIdent iCustomerId, 
											eCustomerTier iCustomerTier,
											INT64 AValue, INT32 SValue )
	{
		TIdent	iAccountOffset;
		INT32	iAccountCount;
		TIdent	iStartingAccount;

		iAccountCount = GetNumberOfAccounts(iCustomerId, iCustomerTier);

		iStartingAccount = GetStartingCA_ID(iCustomerId);

		iAccountOffset = (TIdent) RND.NURnd( iAccountCount, AValue, SValue ) - 1;	// -1 to be between 0 and iAccountCount -1
		return( iStartingAccount + iAccountOffset );
	}

	/*
	*	Get starting account id for a given customer id.		
	*	This is needed for the driver to know what account ids belong to a given customer
	*/
	TIdent GetStartingCA_ID(TIdent CID)
	{
		//start account ids on the next boundary for the new customer
		return ((CID-1) * iMaxAccountsPerCust + 1);
	}

	/*
	*	Get (maximum potential) ending account id for a given customer id.		
	*	This is needed for the driver to restrict query results to active accounts.
	*/
	TIdent GetEndingCA_ID(TIdent CID)
	{
		return (CID * iMaxAccountsPerCust);
	}

	/*
	*	Generate next CA_ID and update state information.
	*/
	TIdent GenerateNextCA_AD()
	{
		if (GetCurrentC_ID() % iDefaultLoadUnitSize == 0)
		{
			InitNextLoadUnit();
		}

		++m_iLastRowNumber;

		if (m_iRowsGeneratedForCust==m_iRowsToGenForCust)
		{	//select next customer id as all the rows for this customer have been generated
			m_cust.GenerateNextC_ID();
			m_addr.GenerateNextAD_ID();	//next address id (to get the one for this customer)
			m_iRowsGeneratedForCust = 0;	//no row generated yet
			//total # of accounts for this customer
			m_iRowsToGenForCust = GetNumberOfAccounts(m_cust.GetCurrentC_ID(), 
													  m_cust.GetC_TIER(m_cust.GetCurrentC_ID()));
			
			m_row.m_ca.CA_ID = GetStartingCA_ID(m_cust.GetCurrentC_ID()) - 1;	//incremented at the end
		}

		++m_iRowsGeneratedForCust;

		//store state info
		m_bMoreRecords = m_cust.MoreRecords() || m_iRowsGeneratedForCust < m_iRowsToGenForCust;

		return ++m_row.m_ca.CA_ID;	//sequential for now
	}
	
	/*
	*	Generate the number (0-2) of additional permission rows for a certain account.
	*	This number is needed by the driver.
	*/
	int GetNumPermsForCA(TIdent CA_ID)
	{
		RNGSEED	OldSeed;
		int		iThreshold;
		int		iNumberOfPermissions;

		OldSeed = m_rnd.GetSeed();

		m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseNumberOfAccountPermissions, CA_ID ));

		iThreshold = m_rnd.RndGenerateIntegerPercentage();

		if (iThreshold <= iPercentAccountAdditionalPermissions_0)
		{
			iNumberOfPermissions = 0;	//60% of accounts have just the owner row permissions
		}
		else
		{
			if (iThreshold <= iPercentAccountAdditionalPermissions_0 + 
				iPercentAccountAdditionalPermissions_1)
			{
				iNumberOfPermissions = 1;	//38% of accounts have one additional permisison row
			}
			else
			{
				iNumberOfPermissions = 2;	//2% of accounts have two additional permission rows
			}
		}

		m_rnd.SetSeed( OldSeed );
		return( iNumberOfPermissions );
	}

	/*
	*	Generate customer ids for ACCOUNT_PERMISSION table for a given account id.
	*	Driver needs to know what those customer ids are based on the account id.
	*/
	void GetCIDsForPermissions(TIdent CA_ID, TIdent Owner_CID, TIdent *CID_1, TIdent *CID_2)
	{
		RNGSEED OldSeed;

		if (CID_1 == NULL)
			return;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseCIDForPermission1, CA_ID ));

		// Select from a fixed range that doesn't depend on the number of customers in the database.
		// This allows not to specify the total number of customers to EGenLoader, only how many
		// a particular instance needs to generate (may be a fraction of total).
		// Note: this is not implemented right now.
		*CID_1 = m_rnd.RndInt64RangeExclude(iDefaultStartFromCustomer,
										iDefaultStartFromCustomer + iAccountPermissionIDRange, 
										Owner_CID);

		if (CID_2 != NULL)
		{
			// NOTE: Reseeding the RNG here for the second CID value. The use of this sequence 
			// is fuzzy because the number of RNG values consumed is dependant on not only the
			// CA_ID, but also the CID value chosen above for the first permission. Using a
			// different sequence here may help prevent potential overlaps that might occur if
			// the same sequence from above were used.
			m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseCIDForPermission2, CA_ID ));
			do	//make sure the second id is different from the first
			{
				*CID_2 = m_rnd.RndInt64RangeExclude(iDefaultStartFromCustomer, 
												iDefaultStartFromCustomer + iAccountPermissionIDRange,
												Owner_CID);
			}
			while (*CID_2 == *CID_1);
		}

		m_rnd.SetSeed( OldSeed );
	}

	/*	
	*	Generate tax id for a given CA_ID.		
	*	This is needed to calculate tax on sale proceeds for the TRADE table.
	*/
	eTaxStatus GetAccountTaxStatus(TIdent iCA_ID)
	{
		RNGSEED	OldSeed;
		eTaxStatus	eCATaxStatus;
		int		iThreshold;

		OldSeed = m_rnd.GetSeed();

		m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseAccountTaxStatus, iCA_ID ));

		iThreshold = m_rnd.RndGenerateIntegerPercentage();
		if (iThreshold <= iPercentAccountTaxStatusNonTaxable)
		{
			eCATaxStatus = eNonTaxable;
		}
		else
		{
			if (iThreshold <= iPercentAccountTaxStatusNonTaxable + iPercentAccountTaxStatusTaxableAndWithhold)
			{
				eCATaxStatus = eTaxableAndWithhold;
			}
			else
			{
				eCATaxStatus = eTaxableAndDontWithhold;
			}
		}

		m_rnd.SetSeed( OldSeed );

		return eCATaxStatus ;
	}

	/*
	*	Get the country and division address codes for the customer that
	*	owns the current account.
	*	These codes are used to get the tax rates and calculate tax on trades
	*	in the TRADE table.
	*/
	void GetDivisionAndCountryCodesForCurrentAccount(int &iDivCode, int &iCtryCode)
	{
		m_addr.GetDivisionAndCountryCodes(iDivCode, iCtryCode);
	}

	/*
	*	Generate a broker id for a certain account.
	*	Used in CTradeGen for updating YTD values.
	*/
	TIdent GenerateBrokerIdForAccount(TIdent iCA_ID)
	{
		//	Customer that own the account (actually, customer id minus 1)
		//
		TIdent	iCustomerId = (iCA_ID - 1) / iMaxAccountsPerCust;

		// Set the starting broker to be the first broker for the current load unit of customers.
		//
		m_iStartFromBroker = (iCustomerId / m_iLoadUnitSize) * m_iBrokersCount;

		// Note: this depends on broker ids being integer numbers from contiguous range.
		// The method of generating broker ids should be in sync with the CBrokerTable.
		return m_rnd.RndNthInt64Range(RNGSeedBaseBrokerId, iCA_ID, 
									iStartingBrokerID + m_iStartFromBroker,
									iStartingBrokerID + m_iStartFromBroker + m_iBrokersCount - 1);
	}

	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		GenerateCARow();
		GenerateAPRows();

		//Return false if all the rows have been generated
		return (MoreRecords());
	}	

	PCUSTOMER_ACCOUNT_ROW	GetCARow() {return &m_row.m_ca;}
	PACCOUNT_PERMISSION_ROW	GetAPRow(UINT i)
	{
		if (i<m_iPermsForCA)
			return &m_row.m_perm[i];
		else
			return NULL;
	}

	int GetCAPermsCount() {return m_iPermsForCA;}//returns the number of permission for the account

	//returns the customer ID for the currently generated CA_ID id
	TIdent			GetCurrentC_ID() {return m_cust.GetCurrentC_ID();}
	eCustomerTier	GetCurrentC_TIER() {return m_cust.GetC_TIER(m_cust.GetCurrentC_ID());}
};

}	// namespace TPCE

#endif //CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H
