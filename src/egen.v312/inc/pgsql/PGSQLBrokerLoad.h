/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for BROKER table.
*/
#ifndef PGSQL_BROKER_LOAD_H
#define PGSQL_BROKER_LOAD_H

namespace TPCE
{

class CPGSQLBrokerLoad : public CPGSQLLoader <BROKER_ROW>
{	
public:
	CPGSQLBrokerLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "BROKER")
		: CPGSQLLoader<BROKER_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.B_ID));			
			buf.push_back(m_row.B_ST_ID);
			buf.push_back(m_row.B_NAME);
			buf.push_back(stringify(m_row.B_NUM_TRADES));
			buf.push_back(stringify(m_row.B_COMM_TOTAL));
	
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

#endif //PGSQL_BROKER_LOAD_H
