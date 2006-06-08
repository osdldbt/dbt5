/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		Doug Johnson
*
*	Description:		This class represents an extension of the
*						MEEApproximation class orginally written by Sergey for
*						EGenLoader.
*						This new class now provides additional functionality
*						for use at runtime by the MEE as well. All of the
*						price/time functionality needed to emulate a securities
*						behavior in the market is captured here.
*						
*
******************************************************************************/

#ifndef MEE_SECURITY_H
#define MEE_SECURITY_H

#include <math.h>

#include "EGenUtilities_stdafx.h"
#include "TradeTypeIDs.h"
#include "SecurityPriceRange.h"

namespace TPCE
{

class CMEESecurity
{
private:
	CRandom		m_rnd;
	CMoney		m_fRangeLow;	// price range start
	CMoney		m_fRangeHigh;	// price range end
	CMoney		m_fRange;		// price range length (high - low)
	int			m_iPeriod;		// time to get to the same price (in seconds)		
	INT32		m_TradingTimeSoFar;	// for picking up where we last left off on the price curve


	CDateTime*	m_pBaseTime;	// Wall clock time corresponding to m_fInitialTime
	CDateTime*	m_pCurrentTime;

	// Mean delay between Pending and Submission times
	// for an immediatelly triggered (in-the-money) limit
	// order. Calculated outside based on scale factor
	// and the number of customers (e.g. MF rate).
	//
	// The actual delay is randomly calculated in the range 
	// [0.5 * Mean .. 1.5 * Mean]
	//
	double		m_fMeanInTheMoneySubmissionDelay;

	// Calculate the "unique" starting offset
	// in the price curve based on the security ID (0-based)
	// 0 corresponds to m_fRangeLow price, 
	// m_fPeriod/2 corresponds to m_fRangeHigh price,
	// m_fPeriod corresponds again to m_fRangeLow price
	inline double InitialTime( TIdent SecurityIndex );

	//	Negative exponential distribution
	//
	inline double NegExp(double fMean);

	//	Calculate time to move from 
	//	the start price to the end price
	//
	double	CalculateTime(	CMoney fStartPrice,
							CMoney fEndPrice,
							int	iStartDirection);

public:
	//	Default constructor (no parameters) to be able 
	//	to allocate an array of security objects.
	//	
	CMEESecurity();

	//	Initialization needed before the first use.
	//
	void Init(RNGSEED Index, INT32 TradingTimeSoFar, CDateTime *pBaseTime, CDateTime *pCurrentTime, double fMeanInTheMoneySubmissionDelay);
	
	//	Calculate the price for a given time
	//
	CMoney	CalculatePrice(TIdent SecurityIndex, double fTime);

	//	Get the current price
	//
	CMoney	GetCurrentPrice( TIdent SecurityIndex );

	// Get the min possible price
	//
	CMoney GetMinPrice( void );

	// Get the max possible price
	CMoney GetMaxPrice( void );

	// Return the expected submission time.
	//
	// Used to calculate triggering time for limit orders.
	//
	double GetSubmissionTime(	TIdent			SecurityIndex,
								double			fPendingTime,
								CMoney			fLimitPrice, 
								eTradeTypeID	TradeType
								);

	// Return the expected completion time
	// and the completion price.
	//
	// Used to calculate completion time for
	// both limit (first must get submission time)
	// and market orders.
	//
	// Equivalent of MEE function sequence
	// 'receive trade' then 'complete the trade request'.
	// Completion time is between 0 and 5 seconds 
	// with 1 sec mean.
	//	
	double GetCompletionTime(	TIdent	SecurityIndex,
								double	fSubmissionTime,
								CMoney*	pCompletionPrice	// output parameter
							);
};

}	// namespace TPCE

#endif //MEE_SECURITY_H
