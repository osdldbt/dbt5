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
*	Description:		Implemenation of the MEEPriceBoard class.
*						See MEEPriceBoad.h for a description.
*
******************************************************************************/

#include "../inc/MEEPriceBoard.h"

using namespace TPCE;

CMEEPriceBoard::CMEEPriceBoard( INT32			TradingTimeSoFar,
								CDateTime*		pBaseTime,
								CDateTime*		pCurrentTime,
								CSecurityFile*	pSecurityFile
								)
: m_fMeanInTheMoneySubmissionDelay( 1.0 )
, m_iNumberOfSecurities( 0 )
, m_pSecurityFile( pSecurityFile )
, m_Security()
{
	// Number of securities is based on "active" customers, as per sub-committee
	// decision to have a scaled-down database look as much as possible as the
	// smaller database.

	// Note that this decision will ensure that some percentage of Trade-Order
	// transactions will *not* be subject to lock contention when accessing the
	// LAST_TRADE table, since they are "outside" of the active range that is
	// updated by the MEE via the Market-Feed transaction.  This may provide an
	// incentive for sponsors to over-build and under-scale at run-time, to the
	// detriment of the benchmark.  If this is the case, then we should require
	// the MEE to run using "configured" securities.

	m_iNumberOfSecurities = m_pSecurityFile->GetActiveSecurityCount();
	m_Security.Init( 0, TradingTimeSoFar, pBaseTime, pCurrentTime, m_fMeanInTheMoneySubmissionDelay );
	m_pSecurityFile->LoadSymbolToIdMap();
}

CMEEPriceBoard::~CMEEPriceBoard(void)
{
}

void	CMEEPriceBoard::GetSymbol(	TIdent	SecurityIndex,
									char*	szOutput,	// output buffer
									INT32	iOutputLen)	// size of the output buffer (including null));
{
	return( m_pSecurityFile->CreateSymbol( SecurityIndex, szOutput, iOutputLen ) );
}

CMoney	CMEEPriceBoard::GetMinPrice( TIdent SecurityIndex )
{
	return( m_Security.GetMinPrice( ));
}

CMoney	CMEEPriceBoard::GetMaxPrice( TIdent SecurityIndex )
{
	return( m_Security.GetMaxPrice( ));
}

CMoney	CMEEPriceBoard::GetCurrentPrice( TIdent SecurityIndex )
{
	return( m_Security.GetCurrentPrice( SecurityIndex ));
}

CMoney	CMEEPriceBoard::GetCurrentPrice( char* pSecuritySymbol )
{
	return( m_Security.GetCurrentPrice( m_pSecurityFile->GetIndex( pSecuritySymbol )));
}

CMoney	CMEEPriceBoard::CalculatePrice( char* pSecuritySymbol, double fTime )
{
	return( m_Security.CalculatePrice( m_pSecurityFile->GetIndex( pSecuritySymbol ), fTime ));
}

double	CMEEPriceBoard::GetSubmissionTime(
							char*			pSecuritySymbol,
							double			fPendingTime,
							CMoney			fLimitPrice, 
							eTradeTypeID	TradeType
							)
{
	return( m_Security.GetSubmissionTime( m_pSecurityFile->GetIndex( pSecuritySymbol ), fPendingTime, 
		fLimitPrice, TradeType ));
}

double	CMEEPriceBoard::GetSubmissionTime(
							TIdent			SecurityIndex,
							double			fPendingTime,
							CMoney			fLimitPrice, 
							eTradeTypeID	TradeType
							)
{
	return( m_Security.GetSubmissionTime( SecurityIndex, fPendingTime, 
		fLimitPrice, TradeType ));
}

double	CMEEPriceBoard::GetCompletionTime(
							TIdent		SecurityIndex,
							double		fSubmissionTime,
							CMoney*		pCompletionPrice	// output parameter
						)
{
	return( m_Security.GetCompletionTime( SecurityIndex, fSubmissionTime,
		pCompletionPrice ));
}
