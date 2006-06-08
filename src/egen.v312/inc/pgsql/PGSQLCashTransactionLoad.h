/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for CASH_TRANSACTION table.
*/
#ifndef PGSQL_CASH_TRANSACTION_LOAD_H
#define PGSQL_CASH_TRANSACTION_LOAD_H

namespace TPCE
{

class CPGSQLCashTransactionLoad : public CPGSQLLoader <CASH_TRANSACTION_ROW>
{	

public:
	CPGSQLCashTransactionLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CASH_TRANSACTION")
		: CPGSQLLoader<CASH_TRANSACTION_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.CT_T_ID));			
			buf.push_back(m_row.CT_DTS.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.CT_AMT));
			buf.push_back(m_row.CT_NAME);
	
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

#endif //PGSQL_CASH_TRANSACTION_LOAD_H
