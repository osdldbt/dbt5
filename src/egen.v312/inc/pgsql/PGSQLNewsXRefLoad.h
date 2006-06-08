/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for NEWS_XREF table.
*/
#ifndef PGSQL_NEWS_XREF_LOAD_H
#define PGSQL_NEWS_XREF_LOAD_H

namespace TPCE
{

class CPGSQLNewsXRefLoad : public CPGSQLLoader <NEWS_XREF_ROW>
{	
public:
	CPGSQLNewsXRefLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "NEWS_XREF")
		: CPGSQLLoader<NEWS_XREF_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};

	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.NX_NI_ID));
			buf.push_back(stringify(m_row.NX_CO_ID));
	
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

#endif //PGSQL_NEWS_XREF_LOAD_H
