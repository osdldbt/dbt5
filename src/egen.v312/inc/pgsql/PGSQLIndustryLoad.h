/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for INDUSTRY table.
*/
#ifndef PGSQL_INDUSTRY_LOAD_H
#define PGSQL_INDUSTRY_LOAD_H

namespace TPCE
{

class CPGSQLIndustryLoad : public CPGSQLLoader <INDUSTRY_ROW>
{	
public:
	CPGSQLIndustryLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "INDUSTRY")
		: CPGSQLLoader<INDUSTRY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.IN_ID));	
			buf.push_back(stringify(m_row.IN_NAME));
			buf.push_back(stringify(m_row.IN_SC_ID));
	
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

#endif //PGSQL_INDUSTRY_LOAD_H
