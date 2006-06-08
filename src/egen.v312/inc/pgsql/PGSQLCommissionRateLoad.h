/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for COMMISSION_RATE table.
*/
#ifndef PGSQL_COMMISSION_RATE_LOAD_H
#define PGSQL_COMMISSION_RATE_LOAD_H

namespace TPCE
{

class CPGSQLCommissionRateLoad : public CPGSQLLoader <COMMISSION_RATE_ROW>
{	
public:
	CPGSQLCommissionRateLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "COMMISSION_RATE")
		: CPGSQLLoader<COMMISSION_RATE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.CR_C_TIER));			
			buf.push_back(m_row.CR_TT_ID);
			buf.push_back(m_row.CR_EX_ID);
			buf.push_back(stringify(m_row.CR_FROM_QTY));
			buf.push_back(stringify(m_row.CR_TO_QTY));
			buf.push_back(stringify(m_row.CR_RATE));
	
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

#endif //PGSQL_COMMISSION_RATE_LOAD_H
