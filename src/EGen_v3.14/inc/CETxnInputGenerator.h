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
 * - Gregory Dake, Doug Johnson, Serge Vasilevskiy, Cecil Reames
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
#include "EGenLogger.h"

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
	PDriverCETxnSettings						m_pDriverCETxnSettings;
	CBaseLogger*								m_pLogger;

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
	void		Initialize( PDriverCETxnSettings pTxnParamSettings );

	void		GenerateNonUniformRandomCustomerId(TIdent &iCustomerId, eCustomerTier &iCustomerTier);
	TIdent		GenerateNonUniformRandomCustomerAccountId( INT32 AValue, INT32 SValue );
	TIdent		GenerateRandomCustomerAccountId(void);
	TIdent		GenerateNonUniformTradeID(INT32 AValue, INT32 SValue);
	void		GenerateNonUniformTradeDTS(TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT32 AValue, INT32 SValue);

public:
	// Constructor - no partitioning by C_ID
	CCETxnInputGenerator( CInputFiles &inputFiles,
				TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				CBaseLogger *pLogger,
				PDriverCETxnSettings pDriverCETxnTunables );

	// Constructor - no partitioning by C_ID, RNG seed provided
	CCETxnInputGenerator( CInputFiles &inputFiles,
				TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				RNGSEED RNGSeed,
				CBaseLogger *pLogger,
				PDriverCETxnSettings pDriverCETxnTunables );

	// Constructor - partitioning by C_ID
	CCETxnInputGenerator( CInputFiles &inputFiles,
				TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				CBaseLogger *pLogger,
				PDriverCETxnSettings pDriverCETxnTunables );

	// Constructor - partitioning by C_ID, RNG seed provided
	CCETxnInputGenerator( CInputFiles &inputFiles,
				TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				RNGSEED RNGSeed,
				CBaseLogger *pLogger,
				PDriverCETxnSettings pDriverCETxnTunables );

	RNGSEED	GetRNGSeed( void );
	void	SetRNGSeed( RNGSEED RNGSeed );

	void	UpdateTunables();

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
