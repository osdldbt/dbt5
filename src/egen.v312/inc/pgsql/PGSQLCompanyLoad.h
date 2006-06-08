/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for COMPANY table.
*/
#ifndef PGSQL_COMPANY_LOAD_H
#define PGSQL_COMPANY_LOAD_H

namespace TPCE
{

class CPGSQLCompanyLoad : public CPGSQLLoader <COMPANY_ROW>
{

public:
	CPGSQLCompanyLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "COMPANY")
		: CPGSQLLoader<COMPANY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.CO_ID));
			buf.push_back(m_row.CO_ST_ID);
			buf.push_back(m_row.CO_NAME);
			buf.push_back(m_row.CO_IN_ID);
			buf.push_back(m_row.CO_SP_RATE);
			buf.push_back(m_row.CO_CEO);
			buf.push_back(stringify(m_row.CO_AD_ID));
			buf.push_back(m_row.CO_DESC);
			buf.push_back(m_row.CO_OPEN_DATE.ToStr(iDateTimeFmt));
			
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

// 	virtual void WriteNextRecord(PT next_record)
// 	{
// 		CopyRow(next_record);	//copy to the bound location inside this class first
// 	
// 		PrepareStmt();
// 	
// 		try {
// 			//NOTE: Using pqxx escape sequence function esc to sanitize strings
// 			m_Txn->exec(m_stmt + m_Txn->esc(m_row.CO_NAME) + "','" + m_Txn->esc(m_row.CO_DESC) + "');");
// 		}
// 		catch (const sql_error &e)
// 		{
// 			cerr << "SQL error: " << e.what() << endl
// 			<< "Query was: '" << e.query() << "'" << endl;
// 		}
// 		catch (const exception &e)
// 		{
// 			cerr<<e.what() << endl;
// 		}
// 	};


};

}	// namespace TPCE

#endif //PGSQL_COMPANY_LOAD_H
