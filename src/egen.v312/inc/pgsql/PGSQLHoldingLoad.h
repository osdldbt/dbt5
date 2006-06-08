/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for HOLDING table.
*/
#ifndef PGSQL_HOLDING_LOAD_H
#define PGSQL_HOLDING_LOAD_H

namespace TPCE
{

class CPGSQLHoldingLoad : public CPGSQLLoader <HOLDING_ROW>
{	

public:
	CPGSQLHoldingLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "HOLDING")
		: CPGSQLLoader<HOLDING_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.H_T_ID));
			buf.push_back(stringify(m_row.H_CA_ID));
			buf.push_back(m_row.H_S_SYMB);
			buf.push_back(m_row.H_DTS.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.H_PRICE));
			buf.push_back(stringify(m_row.H_QTY));
	
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

#endif //PGSQL_HOLDING_LOAD_H
