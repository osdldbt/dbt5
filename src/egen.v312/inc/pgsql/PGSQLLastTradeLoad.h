/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for LAST TRADE table.
*/
#ifndef PGSQL_LAST_TRADE_LOAD_H
#define PGSQL_LAST_TRADE_LOAD_H

namespace TPCE
{

class CPGSQLLastTradeLoad : public CPGSQLLoader <LAST_TRADE_ROW>
{	

public:
	CPGSQLLastTradeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "LAST_TRADE")
		: CPGSQLLoader<LAST_TRADE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.LT_S_SYMB);
			buf.push_back(m_row.LT_DTS.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.LT_PRICE));
			buf.push_back(stringify(m_row.LT_OPEN_PRICE));
			buf.push_back(stringify(m_row.LT_VOL));
	
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

#endif //PGSQL_LAST_TRADE_LOAD_H
