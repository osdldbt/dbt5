/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for WATCH_LIST table.
*/
#ifndef PGSQL_WATCH_LIST_LOAD_H
#define PGSQL_WATCH_LIST_LOAD_H

namespace TPCE
{

class CPGSQLWatchListLoad : public CPGSQLLoader <WATCH_LIST_ROW>
{	
public:
	CPGSQLWatchListLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "WATCH_LIST")
		: CPGSQLLoader<WATCH_LIST_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.WL_ID));
			buf.push_back(stringify(m_row.WL_C_ID));
			
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

#endif //PGSQL_WATCH_LIST_LOAD_H
