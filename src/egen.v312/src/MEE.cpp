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
*	Description:		Implementation of the MEE class.
*						See MEE.h for a description.
*
******************************************************************************/

#include "../inc/MEE.h"

using namespace TPCE;

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
void CMEE::AutoSetRNGSeeds( UINT32 UniqueId )
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
	m_TickerTape.SetRNGSeed( Seed );

	// Set the RNG Id to 1 for the TxnInputGenerator.
	Seed |= 0x0000000100000000ULL;
	m_TradingFloor.SetRNGSeed( Seed );
}

// Constructor - automatic RNG seed generation
CMEE::CMEE( INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CSecurityFile *pSecurityFile, UINT32 UniqueId )
	: m_pSUT( pSUT )
	, m_MEELock()
	, m_PriceBoard( TradingTimeSoFar, &m_BaseTime, &m_CurrentTime, pSecurityFile )
	, m_TickerTape( pSUT, &m_PriceBoard, &m_BaseTime, &m_CurrentTime )
	, m_TradingFloor( pSUT, &m_PriceBoard, &m_TickerTape, &m_BaseTime, &m_CurrentTime )
{
	AutoSetRNGSeeds( UniqueId );
}

// Constructor - RNG seed provided
CMEE::CMEE( INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CSecurityFile *pSecurityFile, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed )
	: m_pSUT( pSUT )
	, m_MEELock()
	, m_PriceBoard( TradingTimeSoFar, &m_BaseTime, &m_CurrentTime, pSecurityFile )
	, m_TickerTape( pSUT, &m_PriceBoard, &m_BaseTime, &m_CurrentTime, TickerTapeRNGSeed )
	, m_TradingFloor( pSUT, &m_PriceBoard, &m_TickerTape, &m_BaseTime, &m_CurrentTime, TradingFloorRNGSeed )
{	
}

CMEE::~CMEE( void )
{
}

RNGSEED CMEE::GetTickerTapeRNGSeed( void )
{
	return( m_TickerTape.GetRNGSeed() );
}

RNGSEED CMEE::GetTradingFloorRNGSeed( void )
{
	return( m_TradingFloor.GetRNGSeed() );
}

void CMEE::SetBaseTime( void )
{
	m_MEELock.ClaimLock();
	m_BaseTime.SetToCurrent( );
	m_MEELock.ReleaseLock();
}

bool CMEE::DisableTickerTape( void )
{
	bool	Result;
	m_MEELock.ClaimLock();
	Result = m_TickerTape.DisableTicker();
	m_MEELock.ReleaseLock();
	return( Result );
}

bool CMEE::EnableTickerTape( void )
{
	bool	Result;
	m_MEELock.ClaimLock();
	Result = m_TickerTape.EnableTicker();
	m_MEELock.ReleaseLock();
	return( Result );
}

INT32 CMEE::GenerateTradeResult( void )
{
	INT32	NextTime;

	m_MEELock.ClaimLock();
	m_CurrentTime.SetToCurrent( );
	NextTime = m_TradingFloor.GenerateTradeResult( );
	m_MEELock.ReleaseLock();
	return( NextTime );
}

INT32 CMEE::SubmitTradeRequest( PTradeRequest pTradeRequest )
{
	INT32 NextTime;

	m_MEELock.ClaimLock();
	m_CurrentTime.SetToCurrent( );
	NextTime = m_TradingFloor.SubmitTradeRequest( pTradeRequest );
	m_MEELock.ReleaseLock();
	return( NextTime );
}
