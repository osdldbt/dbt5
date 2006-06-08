/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	PostgreSQL loader class for ACCOUNT_PERMISSION table.
*/
#ifndef PGSQL_ACCOUNT_PERMISSION_LOAD_H
#define PGSQL_ACCOUNT_PERMISSION_LOAD_H

namespace TPCE
{

class CPGSQLAccountPermissionLoad : public CPGSQLLoader <ACCOUNT_PERMISSION_ROW>
{	
public: 
	CPGSQLAccountPermissionLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "ACCOUNT_PERMISSION")
		: CPGSQLLoader<ACCOUNT_PERMISSION_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};

	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.AP_CA_ID));			
			buf.push_back(m_row.AP_ACL);
			buf.push_back(m_row.AP_TAX_ID);
			buf.push_back(m_row.AP_L_NAME);
			buf.push_back(m_row.AP_F_NAME);
	
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

#endif //PGSQL_ACCOUNT_PERMISSION_LOAD_H
