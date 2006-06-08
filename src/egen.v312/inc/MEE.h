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
*	Description:		This class provides Market Exchange Emulator 
*						functionality. It accepts trade requests for processing;
*						generates a negative exponential delay for each trade to
*						simulate the market processing time; manages the
*						timers for all trades being processed; generates
*						Trade-Result and Market-Feed input data to be used by a 
*						sponsor provided callback interface to the SUT (see
*						MEESUTInterface.h).
*
*						The constructor for this class requires two parameters.
*						- TradingTimeSoFar: the number of seconds for which
*						trades have been run against the database. This allows
*						the MEE to pick up on the price curves from where it 
*						last left off. If doing this isn't important than 0 can
*						passed in.
*						- pSUT: a pointer to an instance of a sponsor provided 
*						subclassing of the CMEESUTInterface class.
*
*						The MEE provides the following entry points.
*
*						- SetBaseTime: used to cordinate the price curves
*						across multiple instances of this class. This method
*						should be called "at the same time" for all instances.
*						This call should be made just prior to starting
*						transactions. There is no return value.
*
*						- SubmitTradeRequest: used for submitting a trade 
*						request into the market. The return value is the number
*						of milliseconds before the next timer is set to expire.
*
*						- GenerateTradeResult: called whenever the current timer
*						has expired (i.e. whenever the number of milliseconds
*						returned by either SubmitTradeRequest or
*						GenerateTradeResult has elapsed). The return value is
*						the number of milliseconds before the next timer is set
*						to expire.
*
*						- DisableTickerTape / EnableTickerTape: by default, the
*						ticker tape functionality of the MEE is enabled. It can
*						be disabled, or re-enabled by calls to these methods.
*						Disabling the ticker tape is useful at the end of a
*						test run to allow processing of submitted orders to
*						continue (Trade-Results) while not generating any
*						ticker tape activity (Market-Feeds).
*
******************************************************************************/

#ifndef MEE_H
#define MEE_H

#include "EGenUtilities_stdafx.h"
#include "MEETradeRequestActions.h"
#include "TxnHarnessStructs.h"
#include "MEEPriceBoard.h"
#include "MEETickerTape.h"
#include "MEETradingFloor.h"
#include "MEESUTInterface.h"

namespace TPCE
{

class CMEE
{
private:

	CMEESUTInterface*	m_pSUT;
	CMEEPriceBoard		m_PriceBoard;
	CMEETickerTape		m_TickerTape;
	CMEETradingFloor	m_TradingFloor;
	CDateTime			m_BaseTime;
	CDateTime			m_CurrentTime;

	CSyncLock			m_MEELock;

	// Automatically generate unique RNG seeds
	void AutoSetRNGSeeds( UINT32 UniqueId );

public:
	static const INT32	NO_OUTSTANDING_TRADES = CMEETradingFloor::NO_OUTSTANDING_TRADES;

	// Constructor - automatic RNG seed generation
	CMEE( INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CSecurityFile *pSecurityFile, UINT32 UniqueId );

	// Constructor - RNG seed provided
	CMEE( INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CSecurityFile *pSecurityFile, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed );

	~CMEE(void);

	RNGSEED	GetTickerTapeRNGSeed( void );
	RNGSEED GetTradingFloorRNGSeed( void );

	void	SetBaseTime( void );

	INT32	SubmitTradeRequest( PTradeRequest pTradeRequest );
	INT32	GenerateTradeResult( void );

	bool	EnableTickerTape( void );
	bool	DisableTickerTape( void );
};

}	// namespace TPCE

#endif //MEE_H
