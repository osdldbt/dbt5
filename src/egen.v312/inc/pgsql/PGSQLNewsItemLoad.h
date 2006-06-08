/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for NEWS_ITEM table.
*/
#ifndef PGSQL_NEWS_ITEM_LOAD_H
#define PGSQL_NEWS_ITEM_LOAD_H

namespace TPCE
{

class CPGSQLNewsItemLoad : public CPGSQLLoader <NEWS_ITEM_ROW>
{	

public:
	CPGSQLNewsItemLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "NEWS_ITEM")
		: CPGSQLLoader<NEWS_ITEM_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};

	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.NI_ID));
			buf.push_back(m_row.NI_HEADLINE);
			buf.push_back(m_row.NI_SUMMARY);
			buf.push_back(m_row.NI_ITEM);
			buf.push_back(m_row.NI_DTS.ToStr(iDateTimeFmt));
			buf.push_back(m_row.NI_SOURCE);
			buf.push_back(m_row.NI_AUTHOR);
	
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

	};


};

}	// namespace TPCE

#endif //PGSQL_NEWS_ITEM_LOAD_H
