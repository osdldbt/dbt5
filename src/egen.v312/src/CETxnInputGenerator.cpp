/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*	Contributions by Doug Johnson, Hewlett-Packard
*	and Matt Emmerton, IBM
*/

#include "../inc/CETxnInputGenerator.h"

using namespace TPCE;

/*
* Constructor - no partitioning by C_ID
*/
CCETxnInputGenerator::CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
						   TIdent iActiveCustomerCount,
						   INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
						   PParameterSettings pParameterSettings )
	: m_rnd(RNGSeedBaseTxnInputGenerator)	//initialize with a default seed
	, m_iConfiguredCustomerCount(iConfiguredCustomerCount)
	, m_iActiveCustomerCount(iActiveCustomerCount)
	, m_iScaleFactor(iScaleFactor)
	, m_iHoursOfInitialTrades(iHoursOfInitialTrades)
	, m_Person(inputFiles)
	, m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount)
	, m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Holdings(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Brokers(inputFiles)
	, m_pCompanies(inputFiles.Company)
	, m_pSecurities(inputFiles.Securities)
	, m_pIndustries(inputFiles.Industry)
	, m_pSectors(inputFiles.Sectors)
	, m_pStatusType(inputFiles.StatusType)
	, m_pTradeType(inputFiles.TradeType)
	, m_pParameterSettings( pParameterSettings )
{
	Initialize( pParameterSettings );
}

/*
* Constructor - no partitioning by C_ID, RNG seed provided
*/
CCETxnInputGenerator::CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
						   TIdent iActiveCustomerCount,
						   INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
						   RNGSEED RNGSeed,
						   PParameterSettings pParameterSettings )
	: m_rnd(RNGSeed)	//to be predictable
	, m_iConfiguredCustomerCount(iConfiguredCustomerCount)
	, m_iActiveCustomerCount(iActiveCustomerCount)
	, m_iScaleFactor(iScaleFactor)
	, m_iHoursOfInitialTrades(iHoursOfInitialTrades)
	, m_Person(inputFiles)
	, m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount)
	, m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Holdings(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Brokers(inputFiles)
	, m_pCompanies(inputFiles.Company)
	, m_pSecurities(inputFiles.Securities)
	, m_pIndustries(inputFiles.Industry)
	, m_pSectors(inputFiles.Sectors)
	, m_pStatusType(inputFiles.StatusType)
	, m_pTradeType(inputFiles.TradeType)
	, m_pParameterSettings( pParameterSettings )
{
	Initialize( pParameterSettings );
}

/*
* Constructor - partitioning by C_ID
*/
CCETxnInputGenerator::CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
							TIdent iActiveCustomerCount,
							TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
							INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
							PParameterSettings pParameterSettings )
	: m_rnd(RNGSeedBaseTxnInputGenerator)	//initialize with a default seed
	, m_iConfiguredCustomerCount(iConfiguredCustomerCount)
	, m_iActiveCustomerCount(iActiveCustomerCount)
	, m_iScaleFactor(iScaleFactor)
	, m_iHoursOfInitialTrades(iHoursOfInitialTrades)
	, m_Person(inputFiles)
	, m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount, iPartitionPercent, iMyStartingCustomerId, iMyCustomerCount)
	, m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Holdings(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Brokers(inputFiles)
	, m_pCompanies(inputFiles.Company)
	, m_pSecurities(inputFiles.Securities)
	, m_pIndustries(inputFiles.Industry)
	, m_pSectors(inputFiles.Sectors)
	, m_pStatusType(inputFiles.StatusType)
	, m_pTradeType(inputFiles.TradeType)
	, m_pParameterSettings( pParameterSettings )
{
	Initialize( pParameterSettings );
}

/*
* Constructor - partitioning by C_ID, RNG seed provided
*/
CCETxnInputGenerator::CCETxnInputGenerator( CInputFiles &inputFiles, TIdent iConfiguredCustomerCount,
							TIdent iActiveCustomerCount,
							TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
							INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
							RNGSEED RNGSeed,
							PParameterSettings pParameterSettings )
	: m_rnd(RNGSeed)	//to be predictable
	, m_iConfiguredCustomerCount(iConfiguredCustomerCount)
	, m_iActiveCustomerCount(iActiveCustomerCount)
	, m_iScaleFactor(iScaleFactor)
	, m_iHoursOfInitialTrades(iHoursOfInitialTrades)
	, m_Person(inputFiles)
	, m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount, iPartitionPercent, iMyStartingCustomerId, iMyCustomerCount)
	, m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Holdings(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
	, m_Brokers(inputFiles)
	, m_pCompanies(inputFiles.Company)
	, m_pSecurities(inputFiles.Securities)
	, m_pIndustries(inputFiles.Industry)
	, m_pSectors(inputFiles.Sectors)
	, m_pStatusType(inputFiles.StatusType)
	, m_pTradeType(inputFiles.TradeType)
	, m_pParameterSettings( pParameterSettings )
{
	Initialize( pParameterSettings );
}

// Performs initialization common to all constructors.
void CCETxnInputGenerator::Initialize( PParameterSettings pParameterSettings )
{
	m_iActiveSecurityCount = m_pSecurities->GetActiveSecurityCount();

	m_iIndustryCount = m_pIndustries->GetSize();

	m_iSectorCount = m_pSectors->GetSize();

	m_iMaxActivePrePopulatedTradeID = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour * ( m_iActiveCustomerCount / m_iScaleFactor )) * 1.01 );  // 1.01 to account for rollbacks

	m_iActiveCompanyCount = m_pCompanies->GetActiveCompanyCount();

	//	Set the start time (time 0) to the base time
	m_StartTime.Set(
		InitialTradePopulationBaseYear,
		InitialTradePopulationBaseMonth,
		InitialTradePopulationBaseDay,
		InitialTradePopulationBaseHour,
		InitialTradePopulationBaseMinute,
		InitialTradePopulationBaseSecond,
		InitialTradePopulationBaseFraction );

	UpdateSettings();
}

RNGSEED CCETxnInputGenerator::GetRNGSeed( void )
{
	return( m_rnd.GetSeed() );
}

void CCETxnInputGenerator::SetRNGSeed( RNGSEED RNGSeed )
{
	m_rnd.SetSeed( RNGSeed );
}

void CCETxnInputGenerator::UpdateSettings( void )
{
	m_iTradeLookupFrame2MaxTimeInMilliSeconds = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour ) - ( m_iScaleFactor * m_pParameterSettings->TL_settings.NumSFOffsetsFromEndTimeFrame2 )) * MsPerSecond;
	m_iTradeLookupFrame3MaxTimeInMilliSeconds = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour ) - ( m_iScaleFactor * m_pParameterSettings->TL_settings.NumSFOffsetsFromEndTimeFrame3 )) * MsPerSecond;
	m_iTradeLookupFrame4MaxTimeInMilliSeconds = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour ) - ( m_iScaleFactor * m_pParameterSettings->TL_settings.NumSFOffsetsFromEndTimeFrame4 )) * MsPerSecond;

	m_iTradeUpdateFrame2MaxTimeInMilliSeconds = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour ) - ( m_iScaleFactor * m_pParameterSettings->TU_settings.NumSFOffsetsFromEndTimeFrame2 )) * MsPerSecond;
	m_iTradeUpdateFrame3MaxTimeInMilliSeconds = (INT64)(( m_iHoursOfInitialTrades * SecondsPerHour ) - ( m_iScaleFactor * m_pParameterSettings->TU_settings.NumSFOffsetsFromEndTimeFrame3 )) * MsPerSecond;

	// Based on 10 * Trade-Order transaction mix percentage.
	// This is currently how the mix levels are set, so use that.
	m_iTradeOrderRollbackLimit = m_pParameterSettings->TxnMixGenerator_settings.TradeOrderMixLevel;
	m_iTradeOrderRollbackLevel = m_pParameterSettings->TO_settings.rollback;
}

/*
*	Generate Non-Uniform customer ID
*/
inline void CCETxnInputGenerator::GenerateNonUniformRandomCustomerId(TIdent &iCustomerId, eCustomerTier &iCustomerTier)
{	
	m_CustomerSelection.GenerateRandomCustomer(iCustomerId, iCustomerTier);
}

/*
*	Generate Non-Uniform customer account ID
*/
TIdent CCETxnInputGenerator::GenerateNonUniformRandomCustomerAccountId( INT64 AValue, INT32 SValue )
{	
	TIdent			iCustomerId;
	eCustomerTier	iCustomerTier;

	m_CustomerSelection.GenerateRandomCustomer(iCustomerId, iCustomerTier);
	return( m_AccsAndPerms.GenerateNonUniformRandomAccountId( m_rnd, iCustomerId, iCustomerTier, AValue, SValue ));
}

/*
*	Generate a trade id to be used in Trade-Lookup Frame 1
*/
TIdent CCETxnInputGenerator::GenerateTradeLookupTradeID()
{
	INT64 TradeId;

	TradeId = m_rnd.NURnd( m_iMaxActivePrePopulatedTradeID,
		m_pParameterSettings->TL_settings.AValueForTradeIDGenFrame1,
		m_pParameterSettings->TL_settings.SValueForTradeIDGenFrame1 );

	// Skip over trade id's that were skipped over during load time.
	if ( m_Holdings.IsAbortedTrade(TradeId) )
	{
		TradeId++;
	}

	return( TradeId );
}

/*
*	Generate a trade id to be used in Trade-Update Frame 1
*/
TIdent CCETxnInputGenerator::GenerateTradeUpdateTradeID()
{
	return( GenerateTradeLookupTradeID( ));
}


void CCETxnInputGenerator::GenerateTradeLookupDTS( TPCE::TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT64 AValue, INT32 SValue )
{
	CDateTime	TradeTime(	InitialTradePopulationBaseYear,
							InitialTradePopulationBaseMonth,
							InitialTradePopulationBaseDay,
							InitialTradePopulationBaseHour,
							InitialTradePopulationBaseMinute,
							InitialTradePopulationBaseSecond,
							InitialTradePopulationBaseFraction );	//NOTE: Interpretting Fraction as milliseconds,
																	// probably 0 anyway.
	INT64		TradeTimeOffset;
	INT32		DaysToAdd;
	INT32		MsToAdd;

	// Generate random number of seconds from the base time.
	//
	TradeTimeOffset = m_rnd.NURnd( MaxTimeInMilliSeconds, AValue, SValue );

	// The time we have is an offset into the initial pre-populated trading time.
	// This needs to be converted into a "real" time taking into account 8 hour 
	// business days, etc. TradeGen does not appear to skip over weekends, so we
	// don't do it here either.
	DaysToAdd = (INT32) ( TradeTimeOffset / MsPerWorkDay );
	MsToAdd = (INT32) TradeTimeOffset - ( DaysToAdd * MsPerWorkDay );
	// Now convert the number of days into work days.
	// 5 per week - skipping Saturday and Sunday.
	DaysToAdd = ((DaysToAdd / 5) * 7) + (DaysToAdd % 5);

	TradeTime.Add( DaysToAdd, MsToAdd );
	TradeTime.GetTimeStamp( &dts );
}

void CCETxnInputGenerator::GenerateTradeUpdateDTS( TPCE::TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT64 AValue, INT32 SValue )
{
	return( GenerateTradeLookupDTS( dts, MaxTimeInMilliSeconds, AValue, SValue ));
}

/*
*	Generate Broker-Volume transaction input.
*	Return the number of brokers generated.
*/
void CCETxnInputGenerator::GenerateBrokerVolumeInput(TBrokerVolumeTxnInput &TxnReq)
{
	TIdent			iBrokersCount = m_iActiveCustomerCount/iBrokersDiv;
	INT32			iNumBrokers;
	INT32			iCount, i;
	const INT32		iBrokerNameListLen = sizeof(TxnReq.broker_list)/sizeof(TxnReq.broker_list[0]);
	TIdent			B_ID[iBrokerNameListLen];
	INT32			iThreshold;
	INT32			iSectorIndex;

	//init all broker names to null
	for (i = 0; i < iBrokerNameListLen; ++i)
	{
		TxnReq.broker_list[i][0] = '\0';
	}

	iNumBrokers = m_rnd.RndIntRange(1, iBrokerNameListLen);

	iCount = 0;
	do
	{
		//select random broker ID (from active customer range)
		B_ID[iCount] = m_rnd.RndInt64Range(iStartingBrokerID, iBrokersCount);

		for (i = 0; (i < iCount) && (B_ID[i] != B_ID[iCount]); ++i);

		if (i == iCount)	//make sure brokers are distinct
		{
			//put the broker name into the input parameter
			m_Brokers.GenerateBrokerName(B_ID[iCount], TxnReq.broker_list[iCount], sizeof(TxnReq.broker_list[iCount]));
			++iCount;
		}

	} while (iCount < iNumBrokers);

	//select sector name
	iSectorIndex = m_rnd.RndIntRange(0, m_iSectorCount-1);

	strncpy(TxnReq.sector_name, 
			m_pSectors->GetRecord(iSectorIndex)->SC_NAME,
			sizeof(TxnReq.sector_name));

	//select customer tier
	iThreshold = m_rnd.RndGenerateIntegerPercentage();
	
	if (iThreshold <= m_pParameterSettings->BV_settings.tier1)
	{
		TxnReq.cust_tier = 1;
	}
	else
	{
		if (iThreshold <= (m_pParameterSettings->BV_settings.tier1 + m_pParameterSettings->BV_settings.tier2))
			TxnReq.cust_tier = 2;
		else
			TxnReq.cust_tier = 3;
	}	

	// Need to address logging issues more comprehensively.
	// return iNumBrokers;
}

/*
*	Generate Customer-Position transaction input
*/
void CCETxnInputGenerator::GenerateCustomerPositionInput(TCustomerPositionTxnInput &TxnReq)
{
	TIdent			iCustomerId;
	eCustomerTier	iCustomerTier;

	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

	if (m_rnd.RndPercent(m_pParameterSettings->CP_settings.by_tax_id))
	{
		//send tax id instead of customer id
		m_Person.GetTaxID(iCustomerId, TxnReq.tax_id);

		TxnReq.cust_id = 0;	//don't need customer id since filled in the tax id
	}
	else
	{
		// send customer id and not the tax id
		TxnReq.cust_id = iCustomerId;

		TxnReq.tax_id[0] = '\0';
	}

	TxnReq.get_history = m_rnd.RndPercent(m_pParameterSettings->CP_settings.get_history);
	if( TxnReq.get_history )
	{
		TxnReq.acct_id_idx = m_rnd.RndIntRange( 0, m_AccsAndPerms.GetNumberOfAccounts( iCustomerId, iCustomerTier ) - 1 );
	}
	else
	{
		TxnReq.acct_id_idx = -1;
	}
}

/*
*	Generate Market-Watch transaction input
*/
void CCETxnInputGenerator::GenerateMarketWatchInput(TMarketWatchTxnInput &TxnReq)
{
	TIdent			iCustomerId;
	eCustomerTier	iCustomerTier;
	INT32			iThreshold;

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	//have some distribution on what inputs to send
	if (iThreshold <= m_pParameterSettings->MW_settings.by_industry)
	{	
		//send industry name
		strncpy(TxnReq.industry_name, 
			m_pIndustries->GetRecord(m_rnd.RndIntRange(0, m_iIndustryCount-1))->IN_NAME,
			sizeof(TxnReq.industry_name));

		TxnReq.c_id = TxnReq.acct_id = 0;

		if( iBaseCompanyCount < m_iActiveCompanyCount )
		{
			TxnReq.starting_co_id = m_rnd.RndInt64Range( 1, m_iActiveCompanyCount - ( iBaseCompanyCount - 1 ));
			TxnReq.ending_co_id = TxnReq.starting_co_id + ( iBaseCompanyCount - 1 );
		}
		else
		{
			TxnReq.starting_co_id = 1;
			TxnReq.ending_co_id = m_iActiveCompanyCount;
		}
	}
	else
	{
		TxnReq.industry_name[0] = '\0';
		TxnReq.starting_co_id = 0;
		TxnReq.ending_co_id = 0;

		if (iThreshold <= (m_pParameterSettings->MW_settings.by_industry + m_pParameterSettings->MW_settings.by_watch_list))
		{	
			// Send customer id
			// Watch list will be retrieved in the transaction
			//
			GenerateNonUniformRandomCustomerId(TxnReq.c_id, iCustomerTier);

			TxnReq.acct_id = 0;			
		}
		else
		{	
			//send account id
			GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

			m_AccsAndPerms.GenerateRandomAccountId(m_rnd, iCustomerId, iCustomerTier, &TxnReq.acct_id, NULL);

			TxnReq.c_id = 0;
		}
	}			
}

/*
*	Generate Security-Detail transaction input
*/
void CCETxnInputGenerator::GenerateSecurityDetailInput(TSecurityDetailTxnInput &TxnReq)
{
	CDateTime	StartDate(iDailyMarketBaseYear, iDailyMarketBaseMonth, 
						  iDailyMarketBaseDay, iDailyMarketBaseHour, 
						  iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);
	INT32		iStartDay;	// day from the StartDate

	// random symbol
	m_pSecurities->CreateSymbol( m_rnd.RndInt64Range(0, m_iActiveSecurityCount-1), TxnReq.symbol, sizeof( TxnReq.symbol ));

	// Whether or not to access the LOB.
	TxnReq.access_lob_flag = m_rnd.RndPercent( m_pParameterSettings->SD_settings.LOBAccessPercentage );

	// random number of financial rows to return
	TxnReq.max_rows_to_return = m_rnd.RndIntRange(iSecurityDetailMinRows, iSecurityDetailMaxRows);
	
	iStartDay = m_rnd.RndIntRange(0, iDailyMarketTotalRows - TxnReq.max_rows_to_return - 1);
	
	// add the offset
	StartDate.Add(iStartDay, 0);
	
	StartDate.GetTimeStamp(&TxnReq.start_day);
}

/* 
*	Trade-Lookup input generation 
*/
void CCETxnInputGenerator::GenerateTradeLookupInput(TTradeLookupTxnInput &TxnReq)
{
	INT32			iThreshold;

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if( iThreshold <= m_pParameterSettings->TL_settings.do_frame1 )
	{
		// Frame 1
		TxnReq.frame_to_execute = 1;
		TxnReq.max_trades = m_pParameterSettings->TL_settings.MaxRowsFrame1;

		// Generate list of unique trade id's
		int		ii, jj;
		bool	Accepted;
		INT64	TID;

		for( ii = 0; ii < TradeLookupFrame1MaxRows; ii++ )
		{
			Accepted = false;
			while( ! Accepted )
			{
				TID = GenerateTradeLookupTradeID();
				jj = 0;
				while( jj < ii && TxnReq.trade_id[jj] != TID )
				{
					jj++;
				}
				if( jj == ii )
				{
					// We have a unique TID for this batch
					TxnReq.trade_id[ii] = TID;
					Accepted = true;
				}
			}
		}

		// Params not used by this frame /////////////////////////////
		TxnReq.acct_id = 0;											//
		TxnReq.max_acct_id = 0;
		memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));			//
		memset( &TxnReq.trade_dts, 0, sizeof( TxnReq.trade_dts ));	//
		//////////////////////////////////////////////////////////////
	}
	else if( iThreshold <=	m_pParameterSettings->TL_settings.do_frame1 + 
							m_pParameterSettings->TL_settings.do_frame2 )
	{
		// Frame 2
		TxnReq.frame_to_execute = 2;
		TxnReq.acct_id = GenerateNonUniformRandomCustomerAccountId( m_pParameterSettings->TL_settings.AValueForAccountIdFrame2,
																	m_pParameterSettings->TL_settings.SValueForAccountIdFrame2 );
		TxnReq.max_trades = m_pParameterSettings->TL_settings.MaxRowsFrame2;

		GenerateTradeLookupDTS( TxnReq.trade_dts,
			m_iTradeLookupFrame2MaxTimeInMilliSeconds,
			m_pParameterSettings->TL_settings.AValueForTimeGenFrame2,
			m_pParameterSettings->TL_settings.SValueForTimeGenFrame2 );

		// Params not used by this frame /////////////////////////
		TxnReq.max_acct_id = 0;
		memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));		//
		memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id ));	//
		//////////////////////////////////////////////////////////
	}
	else if( iThreshold <=	m_pParameterSettings->TL_settings.do_frame1 + 
							m_pParameterSettings->TL_settings.do_frame2 + 
							m_pParameterSettings->TL_settings.do_frame3 )
	{
		// Frame 3
		TxnReq.frame_to_execute = 3;
		TxnReq.acct_id = GenerateNonUniformRandomCustomerAccountId( m_pParameterSettings->TL_settings.AValueForAccountIdFrame3,
																	m_pParameterSettings->TL_settings.SValueForAccountIdFrame3 );
		GenerateTradeLookupDTS( TxnReq.trade_dts,
			m_iTradeLookupFrame3MaxTimeInMilliSeconds,
			m_pParameterSettings->TL_settings.AValueForTimeGenFrame3,
			m_pParameterSettings->TL_settings.SValueForTimeGenFrame3 );

		// Params not used by this frame /////////////////////////
		TxnReq.max_trades = 0;									//
		TxnReq.max_acct_id = 0;
		memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));		//
		memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id ));	//
		//////////////////////////////////////////////////////////
	}
	else
	{
		// Frame 4
		TxnReq.frame_to_execute = 4;
		TxnReq.max_trades = m_pParameterSettings->TL_settings.MaxRowsFrame4;

		m_pSecurities->CreateSymbol( m_rnd.NURnd( m_iActiveSecurityCount-1, 
												m_pParameterSettings->TL_settings.AValueForSymbolFrame4, 
												m_pParameterSettings->TL_settings.SValueForSymbolFrame4 ),
									TxnReq.symbol, 
									sizeof( TxnReq.symbol ));

		GenerateTradeLookupDTS( TxnReq.trade_dts,
			m_iTradeLookupFrame4MaxTimeInMilliSeconds,
			m_pParameterSettings->TL_settings.AValueForTimeGenFrame4,
			m_pParameterSettings->TL_settings.SValueForTimeGenFrame4 );

		TxnReq.max_acct_id = m_AccsAndPerms.GetEndingCA_ID( m_iActiveCustomerCount );
		// Params not used by this frame /////////////////////////
		TxnReq.acct_id = 0;										//
		memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id ));	//
		//////////////////////////////////////////////////////////
	}
}

/*
* Generate input for the Trade-Order (Market or Limit) transaction.
*/
void CCETxnInputGenerator::GenerateTradeOrderInput(TTradeOrderTxnInput &TxnReq, INT32 &iTradeType, bool &bExecutorIsAccountOwner)
{
	TIdent			iCustomerId;	//owner
	eCustomerTier	iCustomerTier;
	TIdent			CID_1, CID_2;
	bool			bMarket;
	INT32			iAdditionalPerms;
	INT32			iSymbIndex;	
	TIdent			iFlatFileSymbIndex;
	eTradeTypeID	eTradeType;
	
	// Generate random customer
	//
	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

	// Generate random account id and security index
	//
	m_Holdings.GenerateRandomAccountSecurity(iCustomerId, 
											iCustomerTier, 
											&TxnReq.acct_id,
											&iFlatFileSymbIndex,
											&iSymbIndex);	

	//find out how many permission rows there are for this account (in addition to the owner's)
	iAdditionalPerms = m_AccsAndPerms.GetNumPermsForCA(TxnReq.acct_id);
	//distribution same as in the loader for now
	if (iAdditionalPerms == 0)
	{	//select the owner
		m_Person.GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name, 
									TxnReq.exec_l_name, TxnReq.exec_tax_id);

		bExecutorIsAccountOwner = true;
	}
	else
	{	
		// If there is more than one permission set on the account,
		// have some distribution on whether the executor is still
		// the account owner, or it is one of the additional permissions.
		if ( m_rnd.RndPercent(m_pParameterSettings->TO_settings.exec_is_owner) )
		{
			m_Person.GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name, 
										TxnReq.exec_l_name, TxnReq.exec_tax_id);

			bExecutorIsAccountOwner = true;
		}
		else
		{
			if (iAdditionalPerms == 1)
			{
				//select the first non-owner
				m_AccsAndPerms.GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, &CID_1, NULL);

				m_Person.GetFirstLastAndTaxID(CID_1, TxnReq.exec_f_name, 
					TxnReq.exec_l_name, TxnReq.exec_tax_id);			
			}
			else
			{
				//select the second non-owner
				m_AccsAndPerms.GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, &CID_1, &CID_2);			
				//generate third account permission row
				m_Person.GetFirstLastAndTaxID(CID_2, TxnReq.exec_f_name,
					TxnReq.exec_l_name, TxnReq.exec_tax_id);
			}

			bExecutorIsAccountOwner = false;
		}
	}		
	
	// Select either stock symbol or company from the securities flat file.
	//
		
	//have some distribution on the company/symbol input preference
	if (m_rnd.RndPercent(m_pParameterSettings->TO_settings.security_by_symbol))
	{
		//Submit the symbol
		m_pSecurities->CreateSymbol( iFlatFileSymbIndex, TxnReq.symbol, sizeof( TxnReq.symbol ));

		TxnReq.co_name[0] = '\0';
		TxnReq.issue[0] = '\0';		
	}
	else
	{
		//Submit the company name
		m_pCompanies->CreateName( m_pSecurities->GetCompanyIndex( iFlatFileSymbIndex ), TxnReq.co_name, sizeof( TxnReq.co_name ));
	
		strncpy(TxnReq.issue, m_pSecurities->GetRecord(iFlatFileSymbIndex)->S_ISSUE,
				sizeof(TxnReq.issue));

		TxnReq.symbol[0] = '\0';		
	}

	TxnReq.trade_qty = cTRADE_QTY_SIZES[m_rnd.RndIntRange(0, cNUM_TRADE_QTY_SIZES - 1)];
	TxnReq.requested_price = m_rnd.RndDoubleIncrRange(fMinSecPrice, fMaxSecPrice, 0.01);

	// Determine whether Market or Limit order
	bMarket = m_rnd.RndPercent(m_pParameterSettings->TO_settings.market);

	//Determine whether Buy or Sell trade
	if (m_rnd.RndPercent(m_pParameterSettings->TO_settings.buy_orders))
	{
		if (bMarket)
		{
			//Market Buy
			eTradeType = eMarketBuy;			
		}
		else
		{
			//Limit Buy
			eTradeType = eLimitBuy;			
		}		

		// Set margin or cash for Buy
		TxnReq.type_is_margin = m_rnd.RndPercent(m_pParameterSettings->TO_settings.type_is_margin);
	}
	else
	{
		if (bMarket)
		{
			//Market Sell
			eTradeType = eMarketSell;
		}
		else
		{
			// determine whether the Limit Sell is a Stop Loss
			if (m_rnd.RndPercent(m_pParameterSettings->TO_settings.stop_loss))
			{
				//Stop Loss
				eTradeType = eStopLoss;
			}
			else
			{
				//Limit Sell
				eTradeType = eLimitSell;
			}
		}		

		TxnReq.type_is_margin = false;	//all sell orders are cash
	}			
	iTradeType = eTradeType;

	// Distribution of last-in-first-out flag
	TxnReq.is_lifo = m_rnd.RndPercent(m_pParameterSettings->TO_settings.lifo);

	// Copy the trade type id from the flat file
	strncpy(TxnReq.trade_type_id, 
			(m_pTradeType->GetRecord(eTradeType))->TT_ID,
			sizeof(TxnReq.trade_type_id));

	// Copy the status type id's from the flat file
	strncpy(TxnReq.st_pending_id, 
			(m_pStatusType->GetRecord(ePending))->ST_ID,
			sizeof(TxnReq.st_pending_id));
	strncpy(TxnReq.st_submitted_id, 
			(m_pStatusType->GetRecord(eSubmitted))->ST_ID,
			sizeof(TxnReq.st_submitted_id));

	TxnReq.roll_it_back = ( m_iTradeOrderRollbackLevel >= m_rnd.RndIntRange( 1, m_iTradeOrderRollbackLimit ));

	// Need to address logging more comprehensively.
	//return eTradeType;
}

/*
*	Generate Trade-Status transaction input.
*/
void CCETxnInputGenerator::GenerateTradeStatusInput(TTradeStatusTxnInput &TxnReq)
{
	TIdent			iCustomerId;
	eCustomerTier	iCustomerTier;
	
	//select customer id first
	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);
	
	//select random account id
	m_AccsAndPerms.GenerateRandomAccountId(m_rnd, iCustomerId, iCustomerTier, &TxnReq.acct_id, NULL);
}

/* 
*	Trade-Update input generation 
*/
void CCETxnInputGenerator::GenerateTradeUpdateInput(TTradeUpdateTxnInput &TxnReq)
{
	INT32			iThreshold;

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if( iThreshold <= m_pParameterSettings->TU_settings.do_frame1 )
	{
		// Frame 1
		TxnReq.frame_to_execute = 1;
		TxnReq.max_trades = m_pParameterSettings->TU_settings.MaxRowsFrame1;
		TxnReq.max_updates = m_pParameterSettings->TU_settings.MaxRowsToUpdateFrame1;

		// Generate list of unique trade id's
		int		ii, jj;
		bool	Accepted;
		INT64	TID;

		for( ii = 0; ii < TradeUpdateFrame1MaxRows; ii++ )
		{
			Accepted = false;
			while( ! Accepted )
			{
				TID = GenerateTradeUpdateTradeID();
				jj = 0;
				while( jj < ii && TxnReq.trade_id[jj] != TID )
				{
					jj++;
				}
				if( jj == ii )
				{
					// We have a unique TID for this batch
					TxnReq.trade_id[ii] = TID;
					Accepted = true;
				}
			}
		}

		// Params not used by this frame /////////////////////////////
		TxnReq.acct_id = 0;											//
		TxnReq.max_acct_id = 0;
		memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));			//
		memset( &TxnReq.trade_dts, 0, sizeof( TxnReq.trade_dts ));	//
		//////////////////////////////////////////////////////////////
	}
	else if( iThreshold <=	m_pParameterSettings->TU_settings.do_frame1 + 
							m_pParameterSettings->TU_settings.do_frame2 )
	{
		// Frame 2
		TxnReq.frame_to_execute = 2;
		TxnReq.max_trades = m_pParameterSettings->TU_settings.MaxRowsFrame2;
		TxnReq.max_updates = m_pParameterSettings->TU_settings.MaxRowsToUpdateFrame2;

		TxnReq.acct_id = GenerateNonUniformRandomCustomerAccountId( m_pParameterSettings->TU_settings.AValueForAccountIdFrame2,
																	m_pParameterSettings->TU_settings.SValueForAccountIdFrame2 );

		GenerateTradeUpdateDTS( TxnReq.trade_dts,
			m_iTradeUpdateFrame2MaxTimeInMilliSeconds,
			m_pParameterSettings->TU_settings.AValueForTimeGenFrame2,
			m_pParameterSettings->TU_settings.SValueForTimeGenFrame2 );

		// Params not used by this frame /////////////////////////
		TxnReq.max_acct_id = 0;
		memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));		//
		memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id ));	//
		//////////////////////////////////////////////////////////
	}
	else
	{
		// Frame 3
		TxnReq.frame_to_execute = 3;
		TxnReq.max_trades = m_pParameterSettings->TU_settings.MaxRowsFrame3;
		TxnReq.max_updates = m_pParameterSettings->TU_settings.MaxRowsToUpdateFrame3;

		m_pSecurities->CreateSymbol( m_rnd.NURnd( m_iActiveSecurityCount-1, 
													m_pParameterSettings->TU_settings.AValueForSymbolFrame3, 
													m_pParameterSettings->TU_settings.SValueForSymbolFrame3 ),
									TxnReq.symbol,
									sizeof( TxnReq.symbol ));

		GenerateTradeUpdateDTS( TxnReq.trade_dts,
			m_iTradeUpdateFrame3MaxTimeInMilliSeconds,
			m_pParameterSettings->TU_settings.AValueForTimeGenFrame3,
			m_pParameterSettings->TU_settings.SValueForTimeGenFrame3 );

		TxnReq.max_acct_id = m_AccsAndPerms.GetEndingCA_ID( m_iActiveCustomerCount ) ;

		// Params not used by this frame /////////////////////////
		TxnReq.acct_id = 0;										//
		memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id ));	//
		//////////////////////////////////////////////////////////
	}
}
