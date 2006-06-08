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
*	Description:		This class provides Customer Emulator functionality.
*						It controls the mix of customer-initiated transactions
*						and generates all required inputs for each of these
*						transactions. These inputs are then made available to
*						a sponsor provided callback interface to the SUT (see
*						CESUTInterface.h). In addition, a set of constants used
*						to uniquely identify each transaction type is exposed.
*
*						The constructor for this class is overloaded. The first
*						form of the constructor accepts 7 inputs, the last one
*						of which is optional (i.e. it has a default value).
*						- pSUT: a pointer to an instance of a sponsor provided 
*						subclassing of the CCESUTInterface class.
*						- inputFiles: a reference to an instance of the 
*						CInputFiles class containing all input files loaded
*						into memory.
*						- iCustomerCount: the total number of customers to
*						emulate. C_IDs will be generated in the range of 
*						1 to iCustomerCount.
*						- iScaleFactor: the number of customers per tpsE. This
*						should match the scale factor used at load time.
*						- iDaysOfInitialTrades: the number of days of initial 
*						trades that was populated during load time.
*						- RandomSeed: seed to be used for the RNG.
*						- pTxnParamSettings: (optional). a pointer to a params
*						structured used to configure the behavior of the CE
*						class.
*
*						The second form of the constructor accepts all of the
*						same inputs as the first form. In addition, however,
*						it accepts 3 additional inputs between iCustomerCount
*						and iScaleFacto that facilitate partitioning by C_ID
*						during runtime.
*						- iMyStartingCustomerId: the starting customer ID for
*						this instance's partition of C_IDs. This ID must be
*						the first ID in a load unit.
*						- iMyCustomerCount: the number of customers in this
*						instance's partition. The nubmer of customers specified
*						must be an integral multiple of the load unit size.
*						- iPartitionPercent: the percentage of C_IDs 
*						that should be generated within this instance's
*						partition. 100-iParititionPercent percent of the C_IDs
*						will be generated across the full range of C_IDs.
*
*						Paritioning Example.
*						Based on a load unit size of 1000, assume the following
*						valid inputs.
*						- iCustomerCount =				5000
*						- iMyStartingCustomerID =		2001
*						- iMyCustomerCount =			2000
*						- iPartitionPercent =			40
*						These setting will configure the CE to generated C_IDs
*						as follows.
*						- 40% of the time in the range [2001 - 4000]
*						- 60% of the time in the range [1-5000]
*
*						The CE provides the following entry point.
*
*						- DoTxn: this entry point will select the next
*						transaction type based on the mix settings, generate
*						all required inputs for the selected transaction type,
*						and provide those inputs to sponsor code at the 
*						appropriate callback interface.
*
******************************************************************************/

#ifndef CE_H
#define CE_H

#include "EGenUtilities_stdafx.h"
#include "CETxnInputGenerator.h"
#include "CETxnMixGenerator.h"
#include "CESUTInterface.h"

namespace TPCE
{

class CCE
{
private:

	TParameterSettings			m_ParameterSettings;
	CCESUTInterface*			m_pSUT;
	CCETxnMixGenerator			m_TxnMixGenerator;
	CCETxnInputGenerator		m_TxnInputGenerator;

	TBrokerVolumeTxnInput		m_BrokerVolumeTxnInput;
	TCustomerPositionTxnInput	m_CustomerPositionTxnInput;
	TMarketWatchTxnInput		m_MarketWatchTxnInput;
	TSecurityDetailTxnInput		m_SecurityDetailTxnInput;
	TTradeLookupTxnInput		m_TradeLookupTxnInput;
	TTradeOrderTxnInput			m_TradeOrderTxnInput;
	TTradeStatusTxnInput		m_TradeStatusTxnInput;
	TTradeUpdateTxnInput		m_TradeUpdateTxnInput;

	// Initialization that is common for all constructors.
	void Initialize( PParameterSettings pParamSettings );

	// Automatically generate unique RNG seeds
	void AutoSetRNGSeeds( UINT32 UniqueId );

public:
	const static INT32 INVALID_TRANSACTION_TYPE	= CCETxnMixGenerator::INVALID_TRANSACTION_TYPE;
	const static INT32 SECURITY_DETAIL			= CCETxnMixGenerator::SECURITY_DETAIL;
	const static INT32 BROKER_VOLUME			= CCETxnMixGenerator::BROKER_VOLUME;
	const static INT32 CUSTOMER_POSITION		= CCETxnMixGenerator::CUSTOMER_POSITION;
	const static INT32 MARKET_WATCH				= CCETxnMixGenerator::MARKET_WATCH;
	const static INT32 TRADE_STATUS				= CCETxnMixGenerator::TRADE_STATUS;
	const static INT32 TRADE_LOOKUP				= CCETxnMixGenerator::TRADE_LOOKUP;
	const static INT32 TRADE_ORDER				= CCETxnMixGenerator::TRADE_ORDER;
	const static INT32 TRADE_UPDATE				= CCETxnMixGenerator::TRADE_UPDATE;
	//Trade-Result and Market-Feed are included for completness.
	const static INT32 MARKET_FEED				= CCETxnMixGenerator::MARKET_FEED;
	const static INT32 TRADE_RESULT				= CCETxnMixGenerator::TRADE_RESULT;

	// Constructor - no partitioning by C_ID, automatic RNG seed generation (requires unique input)
	CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
				UINT32 UniqueId,				
				PParameterSettings pParameterSettings = NULL );

	// Constructor - no partitioning by C_ID, RNG seeds provided
	CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
				RNGSEED TxnMixRNGSeed,
				RNGSEED TxnInputRNGSeed,
				PParameterSettings pParameterSettings = NULL );

	// Constructor - partitioning by C_ID, automatic RNG seed generation (requires unique input)
	CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
				UINT32 UniqueId,
				PParameterSettings pParameterSettings = NULL );

	// Constructor - partitioning by C_ID, RNG seeds provided
	CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
				TIdent iActiveCustomerCount,
				TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
				RNGSEED TxnMixRNGSeed,				
				RNGSEED TxnInputRNGSeed,				
				PParameterSettings pParameterSettings = NULL );

	~CCE( void );

	RNGSEED	GetTxnInputGeneratorRNGSeed( void );
	RNGSEED	GetTxnMixGeneratorRNGSeed( void );
	bool	SetParameterSettings(PParameterSettings pParameterSettings);

	void DoTxn( void );
};

}	// namespace TPCE

#endif //CE_H
