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
 * - Doug Johnson, Cecil Reames, Matt Emmerton
 */

/******************************************************************************
*	Description:		Implementation of CTxnGeneration class.
*						See TxnMixGenerator.h for description.
******************************************************************************/

#include "../inc/CETxnMixGenerator.h"

using namespace TPCE;

CCETxnMixGenerator::CCETxnMixGenerator( PDriverCETxnSettings pDriverCETxnSettings, CBaseLogger* pLogger )
	: m_pDriverCETxnSettings( pDriverCETxnSettings )
	, m_rnd( RNGSeedBaseTxnMixGenerator )	// initialize with default seed
	, m_pLogger( pLogger )
{
	UpdateTunables();
}

CCETxnMixGenerator::CCETxnMixGenerator( PDriverCETxnSettings pDriverCETxnSettings, RNGSEED RNGSeed, CBaseLogger* pLogger )
	: m_pDriverCETxnSettings( pDriverCETxnSettings )
	, m_rnd( RNGSeed )	// seed is provided for us
	, m_pLogger( pLogger )
{
	UpdateTunables();
}

RNGSEED CCETxnMixGenerator::GetRNGSeed( void )
{
	return( m_rnd.GetSeed() );
}

void CCETxnMixGenerator::SetRNGSeed( RNGSEED RNGSeed )
{
	m_rnd.SetSeed( RNGSeed );
}

void CCETxnMixGenerator::UpdateTunables( void )
{
	// Set class members based on established settings.
	m_MarketFeedMix = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.MarketFeedMixLevel;
	m_TradeResultMix = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeResultMixLevel;

	// Add all the weights together
	m_TransactionMixTotalNoMarketFeedNoTradeResult = 
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.BrokerVolumeMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.CustomerPositionMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.MarketWatchMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.SecurityDetailMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeLookupMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeStatusMixLevel +
		m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeUpdateMixLevel;
	m_TransactionMixTotal = m_TransactionMixTotalNoMarketFeedNoTradeResult + m_MarketFeedMix + m_TradeResultMix;

	// Set limits to be used in "if" tests when generating the next transaction
	// type. Note that the order here must match the order of the "if" tests in
	// GenerateNextTxnType().
	m_TradeStatusMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeStatusMixLevel;
	m_MarketWatchMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.MarketWatchMixLevel + m_TradeStatusMixLimit;
	m_SecurityDetailMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.SecurityDetailMixLevel + m_MarketWatchMixLimit;
	m_CustomerPositionMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.CustomerPositionMixLevel + m_SecurityDetailMixLimit;
	m_TradeOrderMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel + m_CustomerPositionMixLimit;
	m_TradeLookupMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeLookupMixLevel + m_TradeOrderMixLimit;
	m_TradeUpdateMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeUpdateMixLevel + m_TradeLookupMixLimit;
	m_BrokerVolumeMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.BrokerVolumeMixLevel + m_TradeUpdateMixLimit;

	// Log Tunables
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->TxnMixGenerator_settings);
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
