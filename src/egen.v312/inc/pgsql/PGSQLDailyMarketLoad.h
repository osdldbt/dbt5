/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	PGSQL Database loader class for DAILY_MARKET table.
*/
#ifndef PGSQL_DAILY_MARKET_LOAD_H
#define PGSQL_DAILY_MARKET_LOAD_H

namespace TPCE
{

class CPGSQLDailyMarketLoad : public CPGSQLLoader <DAILY_MARKET_ROW>
{	

public:
	CPGSQLDailyMarketLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "DAILY_MARKET")
		: CPGSQLLoader<DAILY_MARKET_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};

	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.DM_DATE.ToStr(iDateTimeFmt));
			buf.push_back(m_row.DM_S_SYMB);
			buf.push_back(stringify(m_row.DM_CLOSE));
			buf.push_back(stringify(m_row.DM_HIGH));
			buf.push_back(stringify(m_row.DM_LOW));
			buf.push_back(stringify(m_row.DM_VOL));
	
			m_TW->insert(buf);
			buf.clear();
		}
		catch (const sql_error &e)
		{
			cerr << "SQL error: " << e.what() << endl
			<< "Query was: '" << e.query() << "'" << endl;
		}
		catch (const exception &e)
		{
			cerr<<e.what() << endl;
		}
	}
	
};

}	// namespace TPCE

#endif //PGSQL_DAILY_MARKET_LOAD_H
