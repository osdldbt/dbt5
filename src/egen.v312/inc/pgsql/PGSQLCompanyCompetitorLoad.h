/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for COMPANY_COMPETITOR table.
*/
#ifndef PGSQL_COMPANY_COMPETITOR_LOAD_H
#define PGSQL_COMPANY_COMPETITOR_LOAD_H

namespace TPCE
{

class CPGSQLCompanyCompetitorLoad : public CPGSQLLoader <COMPANY_COMPETITOR_ROW>
{	
public:
	CPGSQLCompanyCompetitorLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "COMPANY_COMPETITOR")
		: CPGSQLLoader<COMPANY_COMPETITOR_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	}


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.CP_CO_ID));			
			buf.push_back(stringify(m_row.CP_COMP_CO_ID));
			buf.push_back(m_row.CP_IN_ID);
	
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

#endif //PGSQL_COMPANY_COMPETITOR_LOAD_H
