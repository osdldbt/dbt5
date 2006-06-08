/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for WATCH_ITEM table.
*/
#ifndef PGSQL_WATCH_ITEM_LOAD_H
#define PGSQL_WATCH_ITEM_LOAD_H

namespace TPCE
{

class CPGSQLWatchItemLoad : public CPGSQLLoader <WATCH_ITEM_ROW>
{	
public:
	CPGSQLWatchItemLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "WATCH_ITEM")
		: CPGSQLLoader<WATCH_ITEM_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.WI_WL_ID));
			buf.push_back(m_row.WI_S_SYMB);
	
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

#endif //PGSQL_WATCH_ITEM_LOAD_H
