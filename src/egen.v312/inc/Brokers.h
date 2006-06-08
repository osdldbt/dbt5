/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing the Brokers table.
*/
#ifndef BROKERS_H
#define BROKERS_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

const int	iBrokerNameIDShift = 1000*1000;	//starting ID to generate names from for brokers

const int		iBrokerInitialTradesYTDMin = 10000;
const int		iBrokerInitialTradesYTDMax = 100000;

const double	fBrokerInitialCommissionYTDMin = 10000.0;
const double	fBrokerInitialCommissionYTDMax = 100000.0;

class CBrokersTable : public TableTemplate<BROKER_ROW>
{
	TIdent				m_iTotalBrokers;	//total number of brokers rows to generate
	TIdent				m_iStartFromBroker;
	CPerson				m_person;
	TStatusTypeFile*	m_StatusTypeFile;	// STATUS_TYPE table from the flat file
	int*				m_pNumTrades;		// array of B_NUM_TRADES values
	double*				m_pCommTotal;		// array of B_COMM_TOTAL values


public:
	//	Constructor.
	//
	CBrokersTable(CInputFiles inputFiles)
		: TableTemplate<BROKER_ROW>()
		, m_iTotalBrokers(0)
		, m_iStartFromBroker(0)
		, m_person(inputFiles)
		, m_StatusTypeFile(inputFiles.StatusType)
		, m_pNumTrades(NULL)
		, m_pCommTotal(NULL)
	{
	};

	//	Destructor.
	//
	~CBrokersTable()
	{
		if (m_pNumTrades != NULL)
		{
			delete[] m_pNumTrades;
		}

		if (m_pCommTotal != NULL)
		{
			delete[] m_pCommTotal;
		}
	}

	/*
	*	Initialization method; required when generating data but not for run-time.
	*/
	void InitForGen(TIdent iCustomerCount, TIdent iStartFromCustomer)
	{
		TIdent i;

		if (m_iTotalBrokers != iCustomerCount / iBrokersDiv)
			
		{
			//	Reallocate arrays for the new number of brokers
			//

			m_iTotalBrokers = iCustomerCount / iBrokersDiv;

			if (m_pNumTrades != NULL)
			{
				delete[] m_pNumTrades;
			}

			m_pNumTrades = new int[(size_t)m_iTotalBrokers];			
			
			if (m_pCommTotal != NULL)
			{
				delete[] m_pCommTotal;
			}

			m_pCommTotal = new double[(size_t)m_iTotalBrokers];			
		}

		//	Initialize array to 0
		//
		if (m_pNumTrades != NULL)
		{
			for (i = 0; i < m_iTotalBrokers; ++i)
			{
				m_pNumTrades[i] = 0;
			}
		}

		//	Initialize array to 0
		//
		if (m_pCommTotal != NULL)
		{
			for (i = 0; i < m_iTotalBrokers; ++i)
			{
				m_pCommTotal[i] = 0.0;
			}
		}

		if (m_iStartFromBroker != iStartFromCustomer / iBrokersDiv)
		{
			m_iStartFromBroker = iStartFromCustomer / iBrokersDiv;
		}

		m_iLastRowNumber = 0;
	};

	void UpdateTradeAndCommissionYTD(TIdent B_ID, int iTradeIncrement, double fCommissionIncrement)
	{
		TIdent	iFirstBrokerId = iStartingBrokerID + m_iStartFromBroker;

		if (   (B_ID >= iFirstBrokerId) 
			&& (B_ID < (iFirstBrokerId + m_iTotalBrokers)))
		{
			m_pNumTrades[B_ID - iFirstBrokerId] += iTradeIncrement;
			m_pCommTotal[B_ID - iFirstBrokerId] += fCommissionIncrement;
		}
	}

	/*
	*	Generates the broker name into the provided buffer.
	*	Exposed mostly for the driver (Broker Volume)
	*/
	void GenerateBrokerName(TIdent B_ID, char *B_NAME, int B_NAME_len)
	{
		sprintf(B_NAME, "%s %c. %s", 
				m_person.GetFirstName(B_ID + iBrokerNameIDShift),
				m_person.GetMiddleName(B_ID + iBrokerNameIDShift),
				m_person.GetLastName(B_ID + iBrokerNameIDShift));
	}

	/*
	*	Generates all column values for the next row.
	*/
	bool GenerateNextRecord()
	{
		TIdent	iFirstBrokerId = iStartingBrokerID + m_iStartFromBroker;

		m_row.B_ID = iFirstBrokerId + m_iLastRowNumber;
		strncpy(m_row.B_ST_ID, m_StatusTypeFile->GetRecord(eActive)->ST_ID, sizeof(m_row.B_ST_ID)-1);
		
		GenerateBrokerName(m_row.B_ID, m_row.B_NAME, sizeof(m_row.B_NAME));
		
		m_row.B_NUM_TRADES = m_pNumTrades[m_row.B_ID - iFirstBrokerId];
		m_row.B_COMM_TOTAL = m_pCommTotal[m_row.B_ID - iFirstBrokerId];


		//Update state info
		++m_iLastRowNumber;
		m_bMoreRecords = m_iLastRowNumber < m_iTotalBrokers;
		//Return false if all the rows have been generated
		return (MoreRecords());
	}
};

}	// namespace TPCE

#endif //BROKERS_H
