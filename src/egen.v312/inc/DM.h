/******************************************************************************
*	(c) Copyright 2005, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		This class provides Data-Maintenance functionality.
*						It generates all necessary inputs for the
*						Data-Maintenance transaction. These inputs are then
*						made available to a sponsor provided callback interface
*						to the SUT (see DMSUTInterface.h).
*
*						The constructor to this class accepts the following
*						parameters.
*
*						- pSUT: a pointer to an instance of a sponsor provided 
*						subclassing of the CCESUTInterface class.
*						- inputFiles: a reference to an instance of the 
*						CInputFiles class containing all input files loaded
*						into memory.
*						- iActiveCustomerCount: the total number of customers
*						to emulate. C_IDs will be generated in the range of 
*						1 to iActiveCustomerCount.
*						- RandomSeed: seed to be used for the RNG.
*
*						The DM class provides the following entry point.
*
*						- DoTxn: this entry point will generate all required
*						inputs and provide those inputs to sponsor code at the 
*						appropriate callback interface.
*
******************************************************************************/

#ifndef DM_H
#define DM_H

#include "EGenLoader_stdafx.h"
#include "EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "DMSUTInterface.h"

namespace TPCE
{

class CDM
{
private:
	CRandom						m_rnd;
	CSecurityFile*				m_pSecurities;
	CCompanyFile*				m_pCompanies;
	TTaxRatesDivisionFile*		m_pTaxRatesDivision;
	TIdent						m_iActiveCustomerCount;
	TIdent						m_iSecurityCount;
	TIdent						m_iCompanyCount;
	INT32						m_iDivisionTaxCount;
	INT32						m_DataMaintenanceTableNum;

	TDataMaintenanceTxnInput	m_TxnInput;
	CDMSUTInterface*			m_pSUT;

	// Automatically generate unique RNG seeds
	void						AutoSetRNGSeeds( UINT32 UniqueId );

	TIdent						GenerateRandomCustomerId();

	TIdent						GenerateRandomCompanyId();

	TIdent						GenerateRandomSecurityId();

	// Initialization that is common for all constructors.
	void						Initialize();

public:
	// Constructor - automatice RNG seed generation
	CDM( CDMSUTInterface *pSUT, CInputFiles &inputFiles, TIdent iActiveCustomerCount, UINT32 UniqueId );

	// Constructor - RNG seed provided
	CDM( CDMSUTInterface *pSUT, CInputFiles &inputFiles, TIdent iActiveCustomerCount, RNGSEED RNGSeed );

	RNGSEED	GetRNGSeed( void );
	void	DoTxn( void );
};

}	// namespace TPCE

#endif	// #ifndef DM_H
