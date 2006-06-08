/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for EXCHANGE table.
*/
#ifndef PGSQL_EXCHANGE_LOAD_H
#define PGSQL_EXCHANGE_LOAD_H

namespace TPCE
{

class CPGSQLExchangeLoad : public CPGSQLLoader <EXCHANGE_ROW>
{	
public:
	CPGSQLExchangeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "EXCHANGE")
		: CPGSQLLoader<EXCHANGE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {		
			buf.push_back(m_row.EX_ID);
			buf.push_back(m_row.EX_NAME);
			buf.push_back(stringify(m_row.EX_NUM_SYMB));
			buf.push_back(stringify(m_row.EX_OPEN));
			buf.push_back(stringify(m_row.EX_CLOSE));
			buf.push_back(m_row.EX_DESC);
			buf.push_back(stringify(m_row.EX_AD_ID));

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

#endif //PGSQL_EXCHANGE_LOAD_H
