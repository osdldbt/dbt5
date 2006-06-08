/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for TRADE table.
*/
#ifndef PGSQL_TRADE_LOAD_H
#define PGSQL_TRADE_LOAD_H

namespace TPCE
{

class CPGSQLTradeLoad : public CPGSQLLoader <TRADE_ROW>
{	

public:
	CPGSQLTradeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "TRADE")
		: CPGSQLLoader<TRADE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.T_ID));
			buf.push_back(m_row.T_DTS.ToStr(iDateTimeFmt));
			buf.push_back(m_row.T_ST_ID);
			buf.push_back(m_row.T_TT_ID);
			buf.push_back((m_row.T_IS_CASH ? "true" : "false"));
			buf.push_back(m_row.T_S_SYMB);
			buf.push_back(stringify(m_row.T_QTY));
			buf.push_back(stringify(m_row.T_BID_PRICE));
			buf.push_back(stringify(m_row.T_CA_ID));
			buf.push_back(m_row.T_EXEC_NAME);
			buf.push_back(stringify(m_row.T_TRADE_PRICE));
			buf.push_back(stringify(m_row.T_CHRG));
			buf.push_back(stringify(m_row.T_COMM));
			buf.push_back(stringify(m_row.T_TAX));
			buf.push_back((m_row.T_LIFO ? "true" : "false"));

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

#endif //PGSQL_TRADE_LOAD_H
