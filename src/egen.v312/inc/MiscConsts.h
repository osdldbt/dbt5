/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Miscellaneous constants used throughout EGenLoader.
*/
#ifndef MISC_CONSTS_H
#define MISC_CONSTS_H

namespace TPCE
{

const int		iMaxHostname = 64;
const int		iMaxDBName = 64;
const int		iMaxPath = 512;

#ifdef COMPILE_PGSQL_LOAD
const int		iMaxPGHost = 128;
const int		iMaxPGDBName = 32;
const int		iMaxPGPort = 32;
const int		iMaxStmt = 512;
const int		iDateTimeFmt = 11;
#endif //COMPILE_PGSQL_LOAD

// NOTE: Changing the initial trade populate base date
// can break code used in CCETxnInputGenerator for generating 
// Trade-Lookup data.
const INT16	InitialTradePopulationBaseYear		= 2005;
const UINT16	InitialTradePopulationBaseMonth		= 1;
const UINT16	InitialTradePopulationBaseDay		= 3;
const UINT16	InitialTradePopulationBaseHour		= 9;
const UINT16	InitialTradePopulationBaseMinute	= 0;
const UINT16	InitialTradePopulationBaseSecond	= 0;
const UINT64	InitialTradePopulationBaseFraction	= 0;

// At what trade count multiple to abort trades.
// One trade in every iAboutTrade block is aborted (trade id is thrown out).
//NOTE: this really is 10 * Trade-Order mix percentage!
//
const int	iAbortTrade = 101;

// Used at load and run time to determine which intial trades
// simulate rollback by "aborting" - I.e. used to skip over a 
// trade ID.
const int	iAbortedTradeModFactor = 51;

// Start date for DAILY_MARKET and FINANCIAL.
//
const int		iDailyMarketBaseYear	= 2000;
const int		iDailyMarketBaseMonth	= 1;
const int		iDailyMarketBaseDay		= 3;
const int		iDailyMarketBaseHour	= 0;
const int		iDailyMarketBaseMinute	= 0;
const int		iDailyMarketBaseSecond	= 0;
const int		iDailyMarketBaseMsec	= 0;

const double	MsPerSecondDivisor  = 1000.000;
const INT32		MsPerSecond			= 1000;
const INT32		SecondsPerMinute	= 60;
const INT32		MinutesPerHour		= 60;
const INT32		HoursPerDay			= 24;
const INT32		HoursPerWorkDay		= 8;

const INT32		SecondsPerHour =	SecondsPerMinute * MinutesPerHour;
const INT32		SecondsPerDay =		SecondsPerMinute * MinutesPerHour * HoursPerDay;
const INT32		SecondsPerWorkDay =	SecondsPerMinute * MinutesPerHour * HoursPerWorkDay;
const INT32		MsPerDay =			SecondsPerDay * MsPerSecond;
const INT32		MsPerWorkDay =		SecondsPerWorkDay * MsPerSecond;

// Range of financial rows to return from Security Detail
const int		iSecurityDetailMinRows = 5;
const int		iSecurityDetailMaxRows = 20;

// Trade-Lookup constants
const INT32 TradeLookupMaxTradeHistoryRowsReturned	= 3;	//Based on the maximum number of status changes a trade can go through.
const INT32 TradeLookupMaxRows				= 30;	// Max number of rows for the frames
const INT32 TradeLookupFrame1MaxRows			= TradeLookupMaxRows;
const INT32 TradeLookupFrame2MaxRows			= TradeLookupMaxRows;
const INT32 TradeLookupFrame3MaxRows			= TradeLookupMaxRows;
const INT32 TradeLookupFrame4MaxRows			= TradeLookupMaxRows;

// Trade-Update constants
const INT32 TradeUpdateMaxTradeHistoryRowsReturned	= 3;	//Based on the maximum number of status changes a trade can go through.
const INT32 TradeUpdateMaxRows				= 30;	// Max number of rows for the frames
const INT32 TradeUpdateFrame1MaxRows			= TradeUpdateMaxRows;
const INT32 TradeUpdateFrame2MaxRows			= TradeUpdateMaxRows;
const INT32 TradeUpdateFrame3MaxRows			= TradeUpdateMaxRows;

// These two arrays are used for platform independence
const char	UpperCaseLetters[]	=	"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const char	Numerals[]			=	"0123456789";
const char	LowerCaseLetters[]	=	"abcdefghijklmnopqrstuvwxyz";
const int	MaxLowerCaseLetters =	sizeof(LowerCaseLetters) - 1;

}	// namespace TPCE

#endif //MISC_CONSTS_H
