/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for SECURITY table.
*/
#ifndef PGSQL_SECURITY_LOAD_H
#define PGSQL_SECURITY_LOAD_H

namespace TPCE
{

class CPGSQLSecurityLoad : public CPGSQLLoader <SECURITY_ROW>
{	

public:
	CPGSQLSecurityLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "SECURITY")
		: CPGSQLLoader<SECURITY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.S_SYMB);
			buf.push_back(m_row.S_ISSUE);
			buf.push_back(m_row.S_ST_ID);
			buf.push_back(m_row.S_NAME);
			buf.push_back(m_row.S_EX_ID);
			buf.push_back(stringify(m_row.S_CO_ID));
			buf.push_back(stringify((int)m_row.S_NUM_OUT));
			buf.push_back(m_row.S_START_DATE.ToStr(iDateTimeFmt));
			buf.push_back(m_row.S_EXCH_DATE.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.S_PE));
			buf.push_back(stringify(m_row.S_52WK.HIGH));
			buf.push_back(m_row.S_52WK.HIGH_DATE.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.S_52WK.LOW));
			buf.push_back(m_row.S_52WK.LOW_DATE.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.S_DIVIDEND));
			buf.push_back(stringify(m_row.S_YIELD));

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

#endif //PGSQL_SECURITY_LOAD_H
