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
*	Description:		Class that implements the ticker tape functionality of
*						the market. This class handles the batching up of 
*						individual ticker entries into a batch suitable for the
*						Market-Feed transaction. For each "real" trade result
*						that gets added to the ticker, a number of artificial
*						entries are padded into the batch. 
*
******************************************************************************/

#ifndef MEE_TICKER_TAPE_H
#define MEE_TICKER_TAPE_H

#include "EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "TimerWheel.h"
#include "MEESUTInterface.h"
#include "MEEPriceBoard.h"

namespace TPCE
{

class CMEETickerTape
{
private:
	CMEESUTInterface*	m_pSUT;
	CMEEPriceBoard*		m_pPriceBoard;
	TMarketFeedTxnInput	m_TxnInput;
	INT32				m_BatchIndex;
	CRandom				m_rnd;
	bool				m_Enabled;

	static const int	LIMIT_TRIGGER_TRADE_QTY;
	static const int	RANDOM_TRADE_QTY_1;
	static const int	RANDOM_TRADE_QTY_2;

	CTimerWheel< TTickerEntry, CMEETickerTape, 900, 1000 >	m_LimitOrderTimers;	//Size wheel for 900 seconds with 1,000 millisecond resolution.
	queue<PTickerEntry>	m_InTheMoneyLimitOrderQ;

	CDateTime*			m_pBaseTime;
	CDateTime*			m_pCurrentTime;

	void AddToBatch( PTickerEntry pTickerEntry );
	void AddArtificialEntries( void );
	void AddLimitTrigger( PTickerEntry pTickerEntry );

	// Performs initialization common to all constructors.
	void	Initialize( void );

public:
	// Constructor - use default RNG seed
	CMEETickerTape( CMEESUTInterface* pSUT, CMEEPriceBoard* pPriceBoard, CDateTime* pBaseTime, CDateTime* pCurrentTime );

	// Constructor - RNG seed provided
	CMEETickerTape( CMEESUTInterface* pSUT, CMEEPriceBoard* pPriceBoard, CDateTime* pBaseTime, CDateTime* pCurrentTime, RNGSEED RNGSeed );

	~CMEETickerTape( void );

	void AddEntry( PTickerEntry pTickerEntry );
	void PostLimitOrder( PTradeRequest pTradeRequest );
	bool DisableTicker( void );
	bool EnableTicker( void );
	eTradeTypeID ConvertTradeTypeIdToEnum( char* pTradeType );

	RNGSEED	GetRNGSeed( void );
	void	SetRNGSeed( RNGSEED RNGSeed );
};

}	// namespace TPCE

#endif //MEE_TICKER_TAPE_H
