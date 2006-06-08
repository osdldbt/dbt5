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
*	Description:		Class that handles all of the trading floor activities
*						for the market. This includes providing functionality
*						for accepting orders that need to be submitted to the
*						trading floor, assigning a processing delay time to 
*						the order, and then completing the order after the 
*						processing delay is over.
*
******************************************************************************/

#ifndef MEE_TRADING_FLOOR_H
#define MEE_TRADING_FLOOR_H

#include "EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "TimerWheel.h"
#include "MEESUTInterface.h"
#include "MEEPriceBoard.h"
#include "MEETickerTape.h"

namespace TPCE
{

class CMEETradingFloor
{
private:
	CMEESUTInterface*										m_pSUT;
	CMEEPriceBoard*											m_pPriceBoard;
	CMEETickerTape*											m_pTickerTape;

	CDateTime*	m_pBaseTime;
	CDateTime*	m_pCurrentTime;

	CTimerWheel< TTradeRequest, CMEETradingFloor, 5, 1 >	m_OrderTimers;	//Size wheel for 5 seconds with 1 millisecond resolution.
	CRandom													m_rnd;
	double													m_OrderProcessingDelayMean;
	static const INT32										m_MaxOrderProcessingDelay = 5;

	double	GenProcessingDelay( double fMean );
	void	SendTradeResult( PTradeRequest pTradeRequest );

public:
	static const INT32	NO_OUTSTANDING_TRADES = CTimerWheel< TTradeRequest, CMEETradingFloor, 5, 1 >::NO_OUTSTANDING_TIMERS;

	// Constructor - use default RNG seed
	CMEETradingFloor( CMEESUTInterface* pSUT, CMEEPriceBoard* pPriceBoard, CMEETickerTape* pTickerTape, CDateTime* pBaseTime, CDateTime* pCurrentTime );

	// Constructor - RNG seed provided
	CMEETradingFloor( CMEESUTInterface* pSUT, CMEEPriceBoard* pPriceBoard, CMEETickerTape* pTickerTape, CDateTime* pBaseTime, CDateTime* pCurrentTime, RNGSEED RNGSeed );

	~CMEETradingFloor( void );

	INT32 SubmitTradeRequest( PTradeRequest pTradeRequest );
	INT32 GenerateTradeResult( void );

	RNGSEED	GetRNGSeed( void );
	void	SetRNGSeed( RNGSEED RNGSeed );
};

}	// namespace TPCE

#endif //MEE_TRADING_FLOOR_H
