/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for TAXRATE table.
*/
#ifndef PGSQL_TAXRATE_LOAD_H
#define PGSQL_TAXRATE_LOAD_H

namespace TPCE
{

class CPGSQLTaxrateLoad : public CPGSQLLoader <TAXRATE_ROW>
{	
public:
	CPGSQLTaxrateLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "TAXRATE")
		: CPGSQLLoader<TAXRATE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.TX_ID);
			buf.push_back(m_row.TX_NAME);
			buf.push_back(stringify(m_row.TX_RATE));
	
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

#endif //PGSQL_TAXRATE_LOAD_H
