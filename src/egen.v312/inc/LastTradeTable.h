/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing the LAST_TRADE table.
*/
#ifndef LAST_TRADE_TABLE_H
#define LAST_TRADE_TABLE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CLastTradeTable : public TableTemplate<LAST_TRADE_ROW>
{
	TIdent				m_iSecurityCount;
	TIdent				m_iStartFromSecurity;
	CSecurityFile*		m_SecurityFile;
	CDateTime			m_date;
	CMEESecurity		m_MEESecurity;
	int					m_iHoursOfInitialTrades;

	/*
	*	LAST_TRADE table row generation
	*/
	void GenerateLastTradeRow()
	{		
		m_SecurityFile->CreateSymbol(m_iLastRowNumber, m_row.LT_S_SYMB, sizeof(m_row.LT_S_SYMB));

		m_row.LT_DTS = m_date;

		m_MEESecurity.Init(m_iLastRowNumber, m_iHoursOfInitialTrades * SecondsPerHour, NULL, NULL, 0);

		m_row.LT_PRICE = m_MEESecurity.CalculatePrice(m_iLastRowNumber, 0).DollarAmount();

		m_row.LT_OPEN_PRICE = m_MEESecurity.CalculatePrice(m_iLastRowNumber, 0).DollarAmount();

		// LT_VOL tracks the trading volume for the current day. Initial population
		// ends on a day boundary, so set LT_VOL to 0 for the start of the next day.
		m_row.LT_VOL = 0;
	}
public:
	/*
	*	Constructor.
	*/
	CLastTradeTable(CInputFiles inputFiles, 
					TIdent		iCustomerCount, 
					TIdent		iStartFromCustomer,
					INT32		iHoursOfInitialTrades)
		: TableTemplate<LAST_TRADE_ROW>()
		, m_SecurityFile(inputFiles.Securities)
		, m_iHoursOfInitialTrades( iHoursOfInitialTrades )		
	{
		m_iSecurityCount = m_SecurityFile->CalculateSecurityCount(iCustomerCount);
		m_iStartFromSecurity = m_SecurityFile->CalculateStartFromSecurity(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromSecurity;
	};

	
	bool GenerateNextRecord()
	{			
		GenerateLastTradeRow();

		++m_iLastRowNumber;

		//Update state info
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromSecurity + m_iSecurityCount);

		return (MoreRecords());
	}
};

}	// namespace TPCE

#endif //LAST_TRADE_TABLE_H
