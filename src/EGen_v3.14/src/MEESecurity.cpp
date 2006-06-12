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
 * - Doug Johnson, Sergey Vasilevskiy
 */

/******************************************************************************
*	Description:		Implementation of the MEESecurity class.
*						See MEESecurity.h for a description.
******************************************************************************/

#include "../inc/MEESecurity.h"

using namespace TPCE;

//	Period of security price change (in seconds)
//	e.g. when the price will repeat.
//
const int iSecPricePeriod = 900;  // 15 minutes

// Mean delay between Submission and Completion times
//
const double fMeanCompletionTimeDelay = 1.0;

// Delay added to the clipped MEE Completion delay
// to simulate SUT-to-MEE and MEE-to-SUT processing delays.
//
const double fCompletionSUTDelay = 1.0;	// seconds

/* 
*	Constructor.
*	Initializes the class using RNG and constants.
* 
*	RETURNS:
*			none
*/
CMEESecurity::CMEESecurity()
: m_fRangeLow(fMinSecPrice)
, m_fRangeHigh(fMaxSecPrice)
, m_fRange(fMaxSecPrice - fMinSecPrice)
, m_iPeriod(iSecPricePeriod)
, m_rnd(RNGSeedBaseMEESecurity)
, m_TradingTimeSoFar( 0 )
, m_pBaseTime( NULL )
, m_pCurrentTime( NULL )
{	
}

/*
*	Initialize before the first use.
*	Separated from constructor in order to have default (no-parameters)
*	constructor.
*	
*	RETURNS:
*			none
*/
void CMEESecurity::Init(
						RNGSEED		Index,	// with unique index to start from a different price (time)
						INT32		TradingTimeSoFar,	// for picking up where we last left off on the price curve
						CDateTime	*pBaseTime,
						CDateTime	*pCurrentTime,

						// Mean delay between Pending and Submission times
						// for an immediatelly triggered (in-the-money) limit
						// order. Calculated outside based on scale factor
						// and the number of customers (e.g. MF rate).
						//
						// The actual delay is randomly calculated in the range 
						// [0.5 * Mean .. 1.5 * Mean]
						//
						double fMeanInTheMoneySubmissionDelay
						)
{
	m_TradingTimeSoFar = TradingTimeSoFar;
	m_pBaseTime = pBaseTime;
	m_pCurrentTime = pCurrentTime;

	m_fMeanInTheMoneySubmissionDelay = fMeanInTheMoneySubmissionDelay;
}

// Calculate the "unique" starting offset
// in the price curve based on the security ID (0-based)
inline double CMEESecurity::InitialTime( TIdent SecurityIndex )
{
	return( (((m_TradingTimeSoFar * MsPerSecond) + (SecurityIndex * 556237 + 253791)) % (iSecPricePeriod * MsPerSecond)) / MsPerSecondDivisor );
}

/*
*	Negative exponential distribution.
*
*	RETURNS:
*			random value according to the negative 
*			exponential distribution with the given mean
*/
inline double CMEESecurity::NegExp(double fMean)
{
	double fResult = (-1.0 * log(m_rnd.RndDouble())) * fMean;

	return fResult;
}

CMoney CMEESecurity::GetCurrentPrice( TIdent SecurityIndex )
{
	return( CalculatePrice( SecurityIndex, *m_pCurrentTime - *m_pBaseTime ));
}

CMoney CMEESecurity::GetMinPrice( void )
{
	return( m_fRangeLow );
}

CMoney CMEESecurity::GetMaxPrice( void )
{
	return( m_fRangeHigh );
}

/*
*	Calculate price at a certain point in time.
*
*	RETURNS:
*			price according to the triangular function
*			that will be achived at the given time
*/
CMoney	CMEESecurity::CalculatePrice(	TIdent SecurityIndex,
										double fTime// seconds from initial time
										)
{
	double  fPeriodTime = (fTime + InitialTime( SecurityIndex )) / (double)m_iPeriod;
	double  fTimeWithinPeriod = (fPeriodTime - (int)fPeriodTime) * (double)m_iPeriod;

	double	fPricePosition; // 0..1 corresponding to m_fRangeLow..m_fRangeHigh
	CMoney  PriceCents;

	if (fTimeWithinPeriod < m_iPeriod / 2)
	{
		fPricePosition = fTimeWithinPeriod / (m_iPeriod / 2);
	}
	else
	{
		fPricePosition = (m_iPeriod - fTimeWithinPeriod) / (m_iPeriod / 2);
	}
	
	PriceCents = m_fRangeLow + m_fRange * fPricePosition;	

	return PriceCents;
}

/*
*	Calculate time required to move between certain prices
*	with certain initial direction of price change.
*
*	RETURNS:
*			seconds required to move from the start price to the end price
*/
double	CMEESecurity::CalculateTime(	
										CMoney fStartPrice,
										CMoney fEndPrice,
										int	iStartDirection)
{
	int		iHalfPeriod = m_iPeriod / 2;

	// Distance on the price curve from StartPrice to EndPrice (in dollars)
	//
	CMoney fDistance;	

	// Amount of time (in seconds) needed to move $1 on the price curve.
	// In half a period the price moves over the entire price range.
	//
	double fSpeed = iHalfPeriod / m_fRange.DollarAmount();

	if (fEndPrice > fStartPrice)
	{
		if (iStartDirection > 0)
		{			
			fDistance = fEndPrice - fStartPrice;
		}
		else
		{			
			fDistance = (fStartPrice - m_fRangeLow) + (fEndPrice - m_fRangeLow);
		}
	}
	else
	{
		if (iStartDirection > 0)
		{			
			fDistance = (m_fRangeHigh - fStartPrice) + (m_fRangeHigh - fEndPrice);
		}
		else
		{			
			fDistance = fStartPrice - fEndPrice;
		}
	}

	return fDistance.DollarAmount() * fSpeed;
}

/* 
*	Calculate triggering time for limit orders.
* 
*	RETURNS:
*			the expected submission time
*/
double CMEESecurity::GetSubmissionTime(	TIdent SecurityIndex,
										double			fPendingTime,	// in seconds from time 0
										CMoney			fLimitPrice, 
										eTradeTypeID	TradeType
										)
{
	CMoney fPriceAtPendingTime = CalculatePrice( SecurityIndex, fPendingTime );

	int iDirectionAtPendingTime;

	double fSubmissionTimeFromPending;	// Submission - Pending time difference
	
	//	Check if the order is already in the money
	//	e.g. if the current price is less than the buy price
	//	or the current price is more than the sell price.
	//
	if (	((TradeType == eLimitBuy || TradeType == eStopLoss) && 
			fPriceAtPendingTime <= fLimitPrice)
		|| 
			((TradeType == eLimitSell ) && 
			fPriceAtPendingTime >= fLimitPrice))
	{
		//	Order is in-the-money. Trigger immediatelly.
		//
		fSubmissionTimeFromPending = m_rnd.RndDoubleRange(0.5 * m_fMeanInTheMoneySubmissionDelay, 
														  1.5 * m_fMeanInTheMoneySubmissionDelay);
	}
	else
	{
		if ((int)(fPendingTime + InitialTime( SecurityIndex )) % m_iPeriod < m_iPeriod / 2)
		{
			//	In the first half of the period => price is going up
			//
			iDirectionAtPendingTime = 1;
		}
		else
		{
			//	In the second half of the period => price is going down
			//
			iDirectionAtPendingTime = -1;
		}

		fSubmissionTimeFromPending = CalculateTime(	fPriceAtPendingTime, 
			fLimitPrice, 
			iDirectionAtPendingTime);
	}

	return fPendingTime + fSubmissionTimeFromPending;
}

/* 
*	Return the expected completion time and the completion price.
*	Completion time is between 0 and 5 seconds 
*	with 1 sec mean.
*
*	RETURNS:
*			the approximated completion time for the trade
*
*/	
double CMEESecurity::GetCompletionTime(	TIdent SecurityIndex, 
										double		fSubmissionTime,	// in seconds from time 0
										CMoney*		pCompletionPrice	// out
										)
{
	double	fCompletionDelay = NegExp(fMeanCompletionTimeDelay);

	// Clip at 5 seconds to prevent rare, but really long delays
	//
	if (fCompletionDelay > 5.0)
	{
		fCompletionDelay = 5.0;
	}

	if (pCompletionPrice != NULL)
	{
		*pCompletionPrice = CalculatePrice( SecurityIndex, fSubmissionTime + fCompletionDelay );
	}

	return fSubmissionTime + fCompletionDelay + fCompletionSUTDelay;
}
