/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for FINANCIAL table.
*/
#ifndef PGSQL_FINANCIAL_LOAD_H
#define PGSQL_FINANCIAL_LOAD_H

namespace TPCE
{

class CPGSQLFinancialLoad : public CPGSQLLoader <FINANCIAL_ROW>
{	

public:
	CPGSQLFinancialLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "FINANCIAL")
		: CPGSQLLoader<FINANCIAL_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.FI_CO_ID));
			buf.push_back(stringify(m_row.FI_YEAR));
			buf.push_back(stringify(m_row.FI_QTR));
			buf.push_back(m_row.FI_QTR_START_DATE.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.FI_REVENUE));
			buf.push_back(stringify(m_row.FI_NET_EARN));
			buf.push_back(stringify(m_row.FI_BASIC_EPS));
			buf.push_back(stringify(m_row.FI_DILUT_EPS));
			buf.push_back(stringify(m_row.FI_MARGIN));
			buf.push_back(stringify(m_row.FI_INVENTORY));
			buf.push_back(stringify(m_row.FI_ASSETS));
			buf.push_back(stringify(m_row.FI_LIABILITY));
			buf.push_back(stringify((int)m_row.FI_OUT_BASIC));
			buf.push_back(stringify((int)m_row.FI_OUT_DILUT));
	
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

#endif //PGSQL_FINANCIAL_LOAD_H
