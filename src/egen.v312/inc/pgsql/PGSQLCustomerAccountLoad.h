/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for CUSTOMER_ACCOUNT table.
*/
#ifndef PGSQL_CUSTOMER_ACCOUNT_LOAD_H
#define PGSQL_CUSTOMER_ACCOUNT_LOAD_H

namespace TPCE
{

class CPGSQLCustomerAccountLoad : public CPGSQLLoader <CUSTOMER_ACCOUNT_ROW>
{	
public:
	CPGSQLCustomerAccountLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CUSTOMER_ACCOUNT")
		: CPGSQLLoader<CUSTOMER_ACCOUNT_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};

	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.CA_ID));
			buf.push_back(stringify(m_row.CA_B_ID));
			buf.push_back(stringify(m_row.CA_C_ID));			
			buf.push_back(m_row.CA_NAME);
			buf.push_back(stringify((int)m_row.CA_TAX_ST));
			buf.push_back(stringify(m_row.CA_BAL));			
	
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

#endif //PGSQL_CUSTOMER_ACCOUNT_LOAD_H
