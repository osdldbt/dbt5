/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for SETTLEMENT table.
*/
#ifndef PGSQL_SETTLEMENT_LOAD_H
#define PGSQL_SETTLEMENT_LOAD_H

namespace TPCE
{

class CPGSQLSettlementLoad : public CPGSQLLoader <SETTLEMENT_ROW>
{	

public:
	CPGSQLSettlementLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "SETTLEMENT")
		: CPGSQLLoader<SETTLEMENT_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{		
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.SE_T_ID));			
			buf.push_back(m_row.SE_CASH_TYPE);
			buf.push_back(m_row.SE_CASH_DUE_DATE.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.SE_AMT));
	
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

#endif //PGSQL_SETTLEMENT_LOAD_H
