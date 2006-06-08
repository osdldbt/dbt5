/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Gregory Dake
*
*	Contributors:		Cecil Reames
*						Doug Johnson
*
*	Description:		Implementation of CTxnGeneration class.
*						See TxnMixGenerator.h for description.
*
******************************************************************************/

#include "../inc/CETxnMixGenerator.h"

using namespace TPCE;

CCETxnMixGenerator::CCETxnMixGenerator( PParameterSettings pParameterSettings )
	: m_pParameterSettings( pParameterSettings )
	, m_rnd( RNGSeedBaseTxnMixGenerator )	// initialize with default seed
{
	UpdateSettings();
}

CCETxnMixGenerator::CCETxnMixGenerator( PParameterSettings pParameterSettings, RNGSEED RNGSeed )
	: m_pParameterSettings( pParameterSettings )
	, m_rnd( RNGSeed )	// seed is provided for us
{
	UpdateSettings();
}

RNGSEED CCETxnMixGenerator::GetRNGSeed( void )
{
	return( m_rnd.GetSeed() );
}

void CCETxnMixGenerator::SetRNGSeed( RNGSEED RNGSeed )
{
	m_rnd.SetSeed( RNGSeed );
}

void CCETxnMixGenerator::UpdateSettings( void )
{
	// Set class members based on established settings.
	m_MarketFeedMix = m_pParameterSettings->TxnMixGenerator_settings.MarketFeedMixLevel;
	m_TradeResultMix = m_pParameterSettings->TxnMixGenerator_settings.TradeResultMixLevel;
	// Add all the weights together
	m_TransactionMixTotalNoMarketFeedNoTradeResult = 
		m_pParameterSettings->TxnMixGenerator_settings.BrokerVolumeMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.CustomerPositionMixLevel	+
		m_pParameterSettings->TxnMixGenerator_settings.MarketWatchMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.SecurityDetailMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.TradeLookupMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.TradeOrderMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.TradeStatusMixLevel +
		m_pParameterSettings->TxnMixGenerator_settings.TradeUpdateMixLevel;
	m_TransactionMixTotal = m_TransactionMixTotalNoMarketFeedNoTradeResult + m_MarketFeedMix + m_TradeResultMix;

	// Set limits to be used in "if" tests when generating the next transaction
	// type. Note that the order here must match the order of the "if" tests in
	// GenerateNextTxnType().
	m_TradeStatusMixLimit = m_pParameterSettings->TxnMixGenerator_settings.TradeStatusMixLevel;
	m_MarketWatchMixLimit = m_pParameterSettings->TxnMixGenerator_settings.MarketWatchMixLevel + m_TradeStatusMixLimit;
	m_SecurityDetailMixLimit = m_pParameterSettings->TxnMixGenerator_settings.SecurityDetailMixLevel + m_MarketWatchMixLimit;
	m_CustomerPositionMixLimit = m_pParameterSettings->TxnMixGenerator_settings.CustomerPositionMixLevel + m_SecurityDetailMixLimit;
	m_TradeOrderMixLimit = m_pParameterSettings->TxnMixGenerator_settings.TradeOrderMixLevel + m_CustomerPositionMixLimit;
	m_TradeLookupMixLimit = m_pParameterSettings->TxnMixGenerator_settings.TradeLookupMixLevel + m_TradeOrderMixLimit;
	m_TradeUpdateMixLimit = m_pParameterSettings->TxnMixGenerator_settings.TradeUpdateMixLevel + m_TradeLookupMixLimit;
	m_BrokerVolumeMixLimit = m_pParameterSettings->TxnMixGenerator_settings.BrokerVolumeMixLevel + m_TradeUpdateMixLimit;
}

int CCETxnMixGenerator::GenerateNextTxnType()
{
	//	Get a 32-bit random number
	//	Use random number to select next transaction type
	//  ignoring Trade-Result and Market-Feed frequencies.
	INT32 rnd = m_rnd.RndIntRange( 1, m_TransactionMixTotalNoMarketFeedNoTradeResult );

	// For efficiency, limit testing is ordered from most likely to least
	// likely (assuming default mix settings).
	// NOTE the "if" test ordering must match how limits are set in constructor.
	//
	if( rnd <= m_TradeStatusMixLimit )
		return( TRADE_STATUS );
	else if( rnd <= m_MarketWatchMixLimit )
		return( MARKET_WATCH );
	else if( rnd <= m_SecurityDetailMixLimit )
		return( SECURITY_DETAIL );
	else if( rnd <= m_CustomerPositionMixLimit )
		return( CUSTOMER_POSITION );
	else if( rnd <= m_TradeOrderMixLimit )
		return( TRADE_ORDER );
	else if( rnd <= m_TradeLookupMixLimit )
		return( TRADE_LOOKUP );
	else if( rnd <= m_TradeUpdateMixLimit )
		return( TRADE_UPDATE );
	else if( rnd <= m_BrokerVolumeMixLimit )
		return( BROKER_VOLUME );
	else
		// Really should throw error - once we sort convention out.
		return( INVALID_TRANSACTION_TYPE );
}
