/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Implementation of the CE class.
*						See CE.h for a description.
*
******************************************************************************/

#include "../inc/CE.h"

using namespace TPCE;

// Initialization that is common for all constructors.
void CCE::Initialize( PParameterSettings pParameterSettings )
{
	if( pParameterSettings )
	{
		// If the provided parameter settings are valid, use them.
		// Otherwise use default settings.
		SetParameterSettings( pParameterSettings );
	}
}

// Automatically generate unique RNG seeds.
// The CRandom class uses an unsigned 64-bit value for the seed.
// This routine automatically generates two unique seeds. One is used for
// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
// The 64 bits are used as follows.
//
//	Bits	0 - 31	Caller provided unique unsigned 32-bit id.
//	Bit		32		0 for TxnInputGenerator, 1 for TxnMixGenerator
//	Bits	33 - 43	Number of days since the base time. The base time
//					is set to be January 1 of the most recent year that is
//					a multiple of 5. This allows enough space for the last
//					field, and it makes the algorithm "timeless" by resetting
//					the generated values every 5 years.
//  Bits	44 - 63	Current time of day measured in 1/10's of a second.
//
void CCE::AutoSetRNGSeeds( UINT32 UniqueId )
{
	CDateTime	Now;
	INT32		BaseYear;
	INT32		Tmp1, Tmp2;

	Now.GetYMD( &BaseYear, &Tmp1, &Tmp2 );

	// Set the base year to be the most recent year that was a multiple of 5.
	BaseYear -= ( BaseYear % 5 );
	CDateTime	Base( BaseYear, 1, 1 );	// January 1st in the BaseYear

	// Initialize the seed with the current time of day measured in 1/10's of a second.
	// This will use up to 20 bits.
	RNGSEED	Seed;
	Seed = Now.MSec() / 100;

	// Now add in the number of days since the base time.
	// The number of days in the 5 year period requires 11 bits.
	// So shift up by that much to make room in the "lower" bits.
	Seed <<= 11;
	Seed += Now.DayNo() - Base.DayNo();

	// So far, we've used up 31 bits.
	// Save the "last" bit of the "upper" 32 for the RNG id.
	// In addition, make room for the caller's 32-bit unique id.
	// So shift a total of 33 bits.
	Seed <<= 33;

	// Now the "upper" 32-bits have been set with a value for RNG 0.
	// Add in the sponsor's unique id for the "lower" 32-bits.
	Seed += UniqueId;

	// Set the TxnMixGenerator RNG to the unique seed.
	m_TxnMixGenerator.SetRNGSeed( Seed );

	// Set the RNG Id to 1 for the TxnInputGenerator.
	Seed |= 0x0000000100000000LL;
	m_TxnInputGenerator.SetRNGSeed( Seed );
}

/*
* Constructor - no partitioning by C_ID, automatic RNG seed generation (requires unique input)
*/
CCE::CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
						   TIdent iActiveCustomerCount,	
						   INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
						   UINT32 UniqueId,
						   PParameterSettings pParameterSettings )
	: m_ParameterSettings(*pParameterSettings)
	, m_pSUT( pSUT )
	, m_TxnMixGenerator( &m_ParameterSettings )
	, m_TxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay, &m_ParameterSettings )
{
	Initialize( pParameterSettings );
	AutoSetRNGSeeds( UniqueId );
}

/*
* Constructor - no partitioning by C_ID, RNG seeds provided
*/
CCE::CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
						   TIdent iActiveCustomerCount,	
						   INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
						   RNGSEED TxnMixRNGSeed,
						   RNGSEED TxnInputRNGSeed,
						   PParameterSettings pParameterSettings )
	: m_ParameterSettings(*pParameterSettings)
	, m_pSUT( pSUT )
	, m_TxnMixGenerator( &m_ParameterSettings, TxnMixRNGSeed )
	, m_TxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay, TxnInputRNGSeed, &m_ParameterSettings )
{
	Initialize( pParameterSettings );
}

/*
* Constructor - partitioning by C_ID, automatic RNG seed generation (requires unique input)
*/
CCE::CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
							TIdent iActiveCustomerCount,	
							TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
							INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
							UINT32 UniqueId,
							PParameterSettings pParameterSettings )
	: m_ParameterSettings(*pParameterSettings)
	, m_pSUT( pSUT )
	, m_TxnMixGenerator( &m_ParameterSettings )
	, m_TxnInputGenerator(	inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent,
							iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay, &m_ParameterSettings )
{
	Initialize( pParameterSettings );
	AutoSetRNGSeeds( UniqueId );
}

/*
* Constructor - partitioning by C_ID, RNG seeds provided
*/
CCE::CCE( CCESUTInterface *pSUT, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
							TIdent iActiveCustomerCount,	
							TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
							INT32 iScaleFactor, INT32 iDaysOfInitialTrades,
							RNGSEED TxnMixRNGSeed,
							RNGSEED TxnInputRNGSeed,
							PParameterSettings pParameterSettings )
	: m_ParameterSettings(*pParameterSettings)
	, m_pSUT( pSUT )
	, m_TxnMixGenerator( &m_ParameterSettings, TxnMixRNGSeed )
	, m_TxnInputGenerator(	inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent,
							iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay, TxnInputRNGSeed, &m_ParameterSettings )
{
	Initialize( pParameterSettings );
}


CCE::~CCE( void )
{
}

RNGSEED CCE::GetTxnInputGeneratorRNGSeed( void )
{
	return( m_TxnInputGenerator.GetRNGSeed() );
}

RNGSEED CCE::GetTxnMixGeneratorRNGSeed( void )
{
	return( m_TxnMixGenerator.GetRNGSeed() );
}

bool CCE::SetParameterSettings( PParameterSettings pParameterSettings )
{
	if( pParameterSettings->IsValid() )
	{
		m_ParameterSettings = *pParameterSettings;
		m_TxnMixGenerator.UpdateSettings();
		m_TxnInputGenerator.UpdateSettings();
		return true;
	}
	else
	{
		return false;
	}
}

void CCE::DoTxn( void )
{
	switch( m_TxnMixGenerator.GenerateNextTxnType( ))
	{
	case CCETxnMixGenerator::BROKER_VOLUME:
		m_TxnInputGenerator.GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
		m_pSUT->BrokerVolume( &m_BrokerVolumeTxnInput );
		break;
	case CCETxnMixGenerator::CUSTOMER_POSITION:
		m_TxnInputGenerator.GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
		m_pSUT->CustomerPosition( &m_CustomerPositionTxnInput );
		break;
	case CCETxnMixGenerator::MARKET_WATCH:
		m_TxnInputGenerator.GenerateMarketWatchInput( m_MarketWatchTxnInput );
		m_pSUT->MarketWatch( &m_MarketWatchTxnInput );
		break;
	case CCETxnMixGenerator::SECURITY_DETAIL:
		m_TxnInputGenerator.GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
		m_pSUT->SecurityDetail( &m_SecurityDetailTxnInput );
		break;
	case CCETxnMixGenerator::TRADE_LOOKUP:
		m_TxnInputGenerator.GenerateTradeLookupInput( m_TradeLookupTxnInput );
		m_pSUT->TradeLookup( &m_TradeLookupTxnInput );
		break;
	case CCETxnMixGenerator::TRADE_ORDER:
		bool	bExecutorIsAccountOwner;
		INT32	iTradeType;
		m_TxnInputGenerator.GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
		m_pSUT->TradeOrder( &m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
		break;
	case CCETxnMixGenerator::TRADE_STATUS:
		m_TxnInputGenerator.GenerateTradeStatusInput( m_TradeStatusTxnInput );
		m_pSUT->TradeStatus( &m_TradeStatusTxnInput );
		break;
	case CCETxnMixGenerator::TRADE_UPDATE:
		m_TxnInputGenerator.GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
		m_pSUT->TradeUpdate( &m_TradeUpdateTxnInput );
		break;
	default:
		cerr << "CE: Generated illegal transaction" << endl;
		exit(1);
	}
}
