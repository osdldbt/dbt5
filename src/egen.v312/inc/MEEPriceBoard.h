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
*	Description:		This class provides price board functionality for the 
*						MEE. This allows for the lookup of any security's price
*						at any point in time.
*
******************************************************************************/

#ifndef MEE_PRICE_BOARD_H
#define MEE_PRICE_BOARD_H

#include "EGenUtilities_stdafx.h"
#include "EGenTables_stdafx.h"
#include "MEESecurity.h"

namespace TPCE
{

class CMEEPriceBoard
{

private:

	// Mean delay between Pending and Submission times
	// for an immediatelly triggered (in-the-money) limit order.
	//
	double				m_fMeanInTheMoneySubmissionDelay;
	CMEESecurity		m_Security;
	CSecurityFile*		m_pSecurityFile;

public:
	TIdent				m_iNumberOfSecurities;

	CMEEPriceBoard( INT32			TradingTimeSoFar,
					CDateTime*		pBaseTime,
					CDateTime*		pCurrentTime,
					CSecurityFile*	pSecurityFile
					);
	~CMEEPriceBoard( void );

	void	GetSymbol(	TIdent	SecurityIndex,
						char*	szOutput,		// output buffer
						INT32	iOutputLen);	// size of the output buffer (including null));

	CMoney	GetMinPrice( TIdent SecurityIndex );

	CMoney	GetMaxPrice( TIdent SecurityIndex );

	CMoney	GetCurrentPrice( TIdent SecurityIndex );
	CMoney	GetCurrentPrice( char* pSecuritySymbol );

	CMoney	CalculatePrice( char* pSecuritySymbol, double fTime );

	double	GetSubmissionTime(
								char*			pSecuritySymbol,
								double			fPendingTime,
								CMoney			fLimitPrice, 
								eTradeTypeID	TradeType
								);
	double	GetSubmissionTime(
								TIdent			SecurityIndex,
								double			fPendingTime,
								CMoney			fLimitPrice, 
								eTradeTypeID	TradeType
								);
	double	GetCompletionTime(
								TIdent		SecurityIndex,
								double		fSubmissionTime,
								CMoney*		pCompletionPrice	// output param
							);
};

}	// namespace TPCE

#endif //MEE_PRICE_BOARD_H
