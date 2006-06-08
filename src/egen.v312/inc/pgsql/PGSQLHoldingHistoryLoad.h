/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for HOLDING_HISTORY table.
*/
#ifndef HOLDING_HISTORY_LOAD_H
#define HOLDING_HISTORY_LOAD_H

namespace TPCE
{

class CPGSQLHoldingHistoryLoad : public CPGSQLLoader <HOLDING_HISTORY_ROW>
{
public:
	CPGSQLHoldingHistoryLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "HOLDING_HISTORY")
		: CPGSQLLoader<HOLDING_HISTORY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.HH_H_T_ID));
			buf.push_back(stringify(m_row.HH_T_ID));
			buf.push_back(stringify(m_row.HH_BEFORE_QTY));
			buf.push_back(stringify(m_row.HH_AFTER_QTY));
	
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

#endif //HOLDING_HISTORY_LOAD_H
