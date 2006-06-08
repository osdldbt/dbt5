/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class that generates transaction input data for the Customer Emulator (CE).
*/

#ifndef CE_TXN_INPUT_GENERATOR_H
#define CE_TXN_INPUT_GENERATOR_H

#include "EGenLoader_stdafx.h"
#include "EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "DriverParamSettings.h"

namespace TPCE
{

class CCETxnInputGenerator
{
	CRandom										m_rnd;		//used inside for parameter generation
	CPerson										m_Person;
	CCustomerSelection							m_CustomerSelection;
	CCustomerAccountsAndPermissionsTable		m_AccsAndPerms;
	CHoldingsAndTradesTable						m_Holdings;
	CBrokersTable								m_Brokers;
	CCompanyFile*								m_pCompanies;
	CSecurityFile*								m_pSecurities;
	TIndustryFile*								m_pIndustries;
	TSectorFile*								m_pSectors;
	TStatusTypeFile*							m_pStatusType;
	TTradeTypeFile*								m_pTradeType;
	PParameterSettings							m_pParameterSettings;

	//number of configured and active customers
	TIdent										m_iConfiguredCustomerCount;
	TIdent										m_iActiveCustomerCount;

	INT32										m_iScaleFactor;
	INT32										m_iHoursOfInitialTrades;
	INT64										m_iMaxActivePrePopulatedTradeID;

	INT64										m_iTradeLookupFrame2MaxTimeInMilliSeconds;
	INT64										m_iTradeLookupFrame3MaxTimeInMilliSeconds;
	INT64										m_iTradeLookupFrame4MaxTimeInMilliSeconds;
	
	INT64										m_iTradeUpdateFrame2MaxTimeInMilliSeconds;
	INT64										m_iTradeUpdateFrame3MaxTimeInMilliSeconds;

	//number of securities (scaled based on active customers)
	TIdent										m_iActiveSecurityCount;
	TIdent										m_iActiveCompanyCount;
	//number of industries (from flat file)
	INT32										m_iIndustryCount;
	//number of sector names (from flat file)
	INT32										m_iSectorCount;
	CDateTime									m_StartTime;	// start time of initial trades


	INT32										m_iTradeOrderRollbackLimit;
	INT32										m_iTradeOrderRollbackLevel;

	// Performs initialization common to all constructors.
	void		Initialize( PParameterSettings pTxnParamSettings );

	void		GenerateNonUniformRandomCustomerId(TIdent &iCustomerId, eCustomerTier &iCustomerTier);
	TIdent		GenerateNonUniformRandomCustomerAccountId( INT64 AValue, INT32 SValue );
	TIdent		GenerateTradeLookupTradeID();
	void		GenerateTradeLookupDTS( TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT64 AValue, INT32 SValue );
	TIdent		GenerateTradeUpdateTradeID();
	void		GenerateTradeUpdateDTS( TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT64 AValue, INT32 SValue );

public:
	// Constructor - no partitioning by C_ID
	CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				PParameterSettings pParameterSettings );

	// Constructor - no partitioning by C_ID, RNG seed provided
	CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				RNGSEED RNGSeed,				
				PParameterSettings pParameterSettings );

	// Constructor - partitioning by C_ID
	CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				PParameterSettings pParameterSettings );

	// Constructor - partitioning by C_ID, RNG seed provided
	CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				RNGSEED RNGSeed,				
				PParameterSettings pParameterSettings );

	RNGSEED	GetRNGSeed( void );
	void	SetRNGSeed( RNGSEED RNGSeed );

	void	UpdateSettings();

	void	GenerateBrokerVolumeInput(TBrokerVolumeTxnInput &TxnReq);
	void	GenerateCustomerPositionInput(TCustomerPositionTxnInput &TxnReq);
	void	GenerateMarketWatchInput(TMarketWatchTxnInput &TxnReq);
	void	GenerateSecurityDetailInput(TSecurityDetailTxnInput &TxnReq);
	void	GenerateTradeLookupInput(TTradeLookupTxnInput &TxnReq);
	void	GenerateTradeOrderInput(TTradeOrderTxnInput &TxnReq, INT32 &iTradeType, bool &bExecutorIsAccountOwner);
	void	GenerateTradeStatusInput(TTradeStatusTxnInput &TxnReq);	
	void	GenerateTradeUpdateInput(TTradeUpdateTxnInput &TxnReq);
};

}	// namespace TPCE

#endif	// #ifndef CE_TXN_INPUT_GENERATOR_H
