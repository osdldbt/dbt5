/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for SECTOR table.
*/
#ifndef PGSQL_SECTOR_LOAD_H
#define PGSQL_SECTOR_LOAD_H

namespace TPCE
{

class CPGSQLSectorLoad : public CPGSQLLoader <SECTOR_ROW>
{	
public:
	CPGSQLSectorLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "SECTOR")
		: CPGSQLLoader<SECTOR_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.SC_ID);
			buf.push_back(m_row.SC_NAME);
	
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

#endif //PGSQL_SECTOR_LOAD_H
