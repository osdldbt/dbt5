/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for TRADE_TYPE table.
*/
#ifndef PGSQL_TRADE_TYPE_LOAD_H
#define PGSQL_TRADE_TYPE_LOAD_H

namespace TPCE
{

class CPGSQLTradeTypeLoad : public CPGSQLLoader <TRADE_TYPE_ROW>
{	
public:
	CPGSQLTradeTypeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "TRADE_TYPE")
		: CPGSQLLoader<TRADE_TYPE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.TT_ID);
			buf.push_back(m_row.TT_NAME);
			buf.push_back((m_row.TT_IS_SELL ? "true" : "false"));
			buf.push_back((m_row.TT_IS_MRKT ? "true" : "false"));
	
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

#endif //PGSQL_TRADE_TYPE_LOAD_H
