/******************************************************************************
*	(c) Copyright 2002-2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		Doug Johnson	(HP)
*
*	Description:		This class encapsulates customer tier distribution
*						functions and provides functionality to:
*						- Generate customer tier based on customer ID
*						- Generate non-uniform customer ID
*						- Generate customer IDs in a specified partition, and
*						  outside the specified partition a set percentage of
*						  the time.
*
******************************************************************************/

#ifndef CUSTOMER_SELECTION_H
#define CUSTOMER_SELECTION_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

/*
*	Define customer tier type.
*/
enum eCustomerTier
{
	eCustomerTierOne = 1,
	eCustomerTierTwo,
	eCustomerTierThree
};

class CCustomerSelection
{
	CRandom*	m_pRND;	// external random number generator

	TIdent		m_iStartFromCustomer;
	TIdent		m_iCustomerCount;

	/*
	*	Used when partitioning by C_ID.
	*/
	bool		m_bPartitionByCID;
	int			m_iPartitionPercent;
	TIdent		m_iMyStartFromCustomer;
	TIdent		m_iMyCustomerCount;

	/*
	*	Forward permutation (used to convert ordinal C_ID into real C_ID).
	*/
	TIdent Permute(TIdent iLow, TIdent iHigh);

	/*
	*	Inverse permutation (used to convert real C_ID into it's ordinal number).
	*/
	TIdent InversePermute(TIdent iLow, TIdent iHigh);

	/*
	*	Get lower 3 digits.
	*/
	inline TIdent CLow(TIdent C_ID) { return ((C_ID - 1) % 1000); }

	/*
	*	Get higher digits.
	*/
	inline TIdent CHigh(TIdent C_ID) { return ((C_ID - 1) / 1000); }

	///////////*
	//////////*	Generate a customer ID and the customer's tier.
	//////////*/
	//////////void GenerateCustomerIdAndTier(TIdent &C_ID, eCustomerTier &C_TIER, bool bAcrossEntireRange);

public:

	/*
	*	Default constructor.
	*/
	CCustomerSelection();

	/*
	*	Constructor to set the customer range.
	*/
	CCustomerSelection(	CRandom*	pRND,
						TIdent		iStartFromCustomer,
						TIdent		iCustomerCount);

	/*
	*	Constructor to set subrange when paritioning by C_ID.
	*/
	CCustomerSelection(	CRandom*	pRND,
						TIdent		iStartFromCustomer,
						TIdent		iCustomerCount, 
						int			iPartitionPercent,
						TIdent		iMyStartFromCustomer, 
						TIdent		iMyCustomerCount);

	/*
	*	Re-set the customer range for the parition.
	*/
	void SetPartitionRange(TIdent iStartFromCustomer, TIdent iCustomerCount);

	/*
	*	Return customer tier.
	*/
	eCustomerTier GetTier(TIdent C_ID);

	/*
	*	Return a non-uniform random customer and the associated tier.
	*/
	void GenerateRandomCustomer(TIdent &C_ID, eCustomerTier &C_TIER);
};

}	// namespace TPCE

#endif //CUSTOMER_SELECTION_H
