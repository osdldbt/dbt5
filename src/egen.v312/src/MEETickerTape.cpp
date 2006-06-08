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
*	Description:		Implementation of the MEETickerTape class.
*						See MEETickerTape.h for a description.
*
******************************************************************************/

#include "../inc/MEETickerTape.h"

using namespace TPCE;

const int CMEETickerTape::LIMIT_TRIGGER_TRADE_QTY = 375;
const int CMEETickerTape::RANDOM_TRADE_QTY_1 = 325;
const int CMEETickerTape::RANDOM_TRADE_QTY_2 = 425;

RNGSEED CMEETickerTape::GetRNGSeed( void )
{
	return( m_rnd.GetSeed() );
}

void CMEETickerTape::SetRNGSeed( RNGSEED RNGSeed )
{
	m_rnd.SetSeed( RNGSeed );
}

void CMEETickerTape::Initialize( void )
{
	// Set up status and trade types for Market-Feed input
	//
	// Submitted
	strncpy(m_TxnInput.StatusAndTradeType.status_submitted, 
			"SBMT",
			sizeof(m_TxnInput.StatusAndTradeType.status_submitted));
	// Limit-Buy
	strncpy(m_TxnInput.StatusAndTradeType.type_limit_buy, 
			"TLB",
			sizeof(m_TxnInput.StatusAndTradeType.type_limit_buy));
	// Limit-Sell
	strncpy(m_TxnInput.StatusAndTradeType.type_limit_sell, 
			"TLS",
			sizeof(m_TxnInput.StatusAndTradeType.type_limit_sell));
	// Stop-Loss
	strncpy(m_TxnInput.StatusAndTradeType.type_stop_loss, 
			"TSL",
			sizeof(m_TxnInput.StatusAndTradeType.type_stop_loss));
}

// Constructor - use default RNG seed
CMEETickerTape::CMEETickerTape( CMEESUTInterface* pSUT, CMEEPriceBoard*	pPriceBoard, CDateTime* pBaseTime, CDateTime* pCurrentTime )
	: m_pSUT( pSUT )
	, m_pPriceBoard( pPriceBoard )
	, m_BatchIndex( 0 )
	, m_pBaseTime( pBaseTime )
	, m_pCurrentTime( pCurrentTime )
	, m_rnd( RNGSeedBaseMEETickerTape )
	, m_Enabled( true )
{
	Initialize();
}

// Constructor - RNG seed provided
CMEETickerTape::CMEETickerTape( CMEESUTInterface* pSUT, CMEEPriceBoard*	pPriceBoard, CDateTime* pBaseTime, CDateTime* pCurrentTime, RNGSEED RNGSeed )
	: m_pSUT( pSUT )
	, m_pPriceBoard( pPriceBoard )
	, m_BatchIndex( 0 )
	, m_pBaseTime( pBaseTime )
	, m_pCurrentTime( pCurrentTime )
	, m_rnd( RNGSeed )
	, m_Enabled( true )
{
	Initialize();
}

CMEETickerTape::~CMEETickerTape( void )
{
}

bool CMEETickerTape::DisableTicker( void )
{
	m_Enabled = false;
	return( ! m_Enabled );
}

bool CMEETickerTape::EnableTicker( void )
{
	m_Enabled = true;
	return( m_Enabled );
}

void CMEETickerTape::AddEntry( PTickerEntry pTickerEntry )
{
	if( m_Enabled )
	{
		AddToBatch( pTickerEntry);
		AddArtificialEntries( );
	}
}

void CMEETickerTape::PostLimitOrder( PTradeRequest pTradeRequest )
{
	eTradeTypeID	eTradeType;
	double			CurrentPrice = -1.0;
	PTickerEntry	pNewEntry = new TTickerEntry;

	eTradeType = ConvertTradeTypeIdToEnum( pTradeRequest->trade_type_id );

	pNewEntry->price_quote = pTradeRequest->price_quote;
	strncpy( pNewEntry->symbol, pTradeRequest->symbol, sizeof( pNewEntry->symbol ));
	pNewEntry->trade_qty = LIMIT_TRIGGER_TRADE_QTY;


	CurrentPrice = m_pPriceBoard->GetCurrentPrice( pTradeRequest->symbol ).DollarAmount();

	if((( eTradeType == eLimitBuy || eTradeType == eStopLoss ) &&
			CurrentPrice <= pTradeRequest->price_quote )
		|| 
			(( eTradeType == eLimitSell ) && 
			CurrentPrice >= pTradeRequest->price_quote ))
	{
		// Limit Order is in-the-money.
		pNewEntry->price_quote = CurrentPrice;
		// Make sure everything is up to date.
		m_LimitOrderTimers.ProcessExpiredTimers();
		// Now post the incoming entry.
		m_InTheMoneyLimitOrderQ.push( pNewEntry );
	}
	else
	{
		// Limit Order is not in-the-money.
		pNewEntry->price_quote = pTradeRequest->price_quote;
		double TriggerTimeDelay;
		double fCurrentTime = *m_pCurrentTime - *m_pBaseTime;

		// GetSubmissionTime returns a value relative to time 0, so we
		// need to substract off the value for the current time to get
		// the delay time relative to now.
		TriggerTimeDelay = m_pPriceBoard->GetSubmissionTime(pNewEntry->symbol,
															fCurrentTime,
															pNewEntry->price_quote,
															eTradeType)
							- fCurrentTime;
		m_LimitOrderTimers.StartTimer( TriggerTimeDelay,
			this, &CMEETickerTape::AddLimitTrigger, pNewEntry );
	}
}

void CMEETickerTape::AddLimitTrigger( PTickerEntry pTickerEntry )
{
	m_InTheMoneyLimitOrderQ.push( pTickerEntry );
}

void CMEETickerTape::AddArtificialEntries( void )
{
	TIdent				SecurityIndex;
	TTickerEntry		TickerEntry;
	int					LimitEntryCount;
	int					RandomEntryCount;
	static const int	PaddingLimit = 10;

	LimitEntryCount = 1;
	while( LimitEntryCount < PaddingLimit && !m_InTheMoneyLimitOrderQ.empty() )
	{
		PTickerEntry pEntry = m_InTheMoneyLimitOrderQ.front();
		AddToBatch( pEntry );
		delete pEntry;
		m_InTheMoneyLimitOrderQ.pop();
		LimitEntryCount++;
	}

	for( RandomEntryCount=LimitEntryCount; RandomEntryCount < PaddingLimit; RandomEntryCount++ )
	{
		TickerEntry.trade_qty = ( m_rnd.RndPercent( 50 )) ? RANDOM_TRADE_QTY_1 : RANDOM_TRADE_QTY_2;

		SecurityIndex = m_rnd.RndInt64Range( 0, m_pPriceBoard->m_iNumberOfSecurities - 1 );
		TickerEntry.price_quote = (m_pPriceBoard->GetCurrentPrice( SecurityIndex )).DollarAmount();
		m_pPriceBoard->GetSymbol( SecurityIndex, TickerEntry.symbol, sizeof(TickerEntry.symbol) );

		AddToBatch( &TickerEntry );
	}
}

void CMEETickerTape::AddToBatch( PTickerEntry pTickerEntry )
{
	m_TxnInput.Entries[m_BatchIndex++] = *pTickerEntry;
	if( max_feed_len == m_BatchIndex )
	{
		// Buffer is full, time for Market-Feed.
		m_pSUT->MarketFeed( &m_TxnInput );
		m_BatchIndex = 0;
	}
}

eTradeTypeID CMEETickerTape::ConvertTradeTypeIdToEnum( char* pTradeType )
{
	// Convert character trade type to enumeration
	switch( pTradeType[1] )
	{
	case 'L':
		switch( pTradeType[2] )
		{
		case 'B':
			return( eLimitBuy );
			break;
		case 'S':
			return( eLimitSell );
			break;
		default:
			// Throw exception - should never get here
			break;
		}
		break;
	case 'M':
		switch( pTradeType[2] )
		{
		case 'B':
			return( eMarketBuy );
			break;
		case 'S':
			return( eMarketSell );
			break;
		default:
			// Throw exception - should never get here
			break;
		}
		break;
	case 'S':
		return( eStopLoss );
		break;
	default:
		// Throw exception - should never get here
		assert(false);	// this should never happen
		break;
	}
	// Throw exception - should never get here
	return eMarketBuy;	// silence compiler warning about not all control paths returning a value
}
