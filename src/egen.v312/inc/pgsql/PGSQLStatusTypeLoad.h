/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for STATUS_TYPE table.
*/
#ifndef PGSQL_STATUS_TYPE_LOAD_H
#define PGSQL_STATUS_TYPE_LOAD_H

namespace TPCE
{

class CPGSQLStatusTypeLoad : public CPGSQLLoader <STATUS_TYPE_ROW>
{	
public:
	CPGSQLStatusTypeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "STATUS_TYPE")
		: CPGSQLLoader<STATUS_TYPE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.ST_ID);
			buf.push_back(m_row.ST_NAME);
	
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

#endif //PGSQL_STATUS_TYPE_LOAD_H
