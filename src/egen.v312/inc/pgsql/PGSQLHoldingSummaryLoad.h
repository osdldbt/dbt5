/******************************************************************************
*	(c) Copyright 2005, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		MSSQL ODBC implementation of HOLDING_SUMMARY loader.
*
******************************************************************************/
// 2006 Rilson Nascimento

#ifndef PGSQL_HOLDING_SUMMARY_LOAD_H
#define PGSQL_HOLDING_SUMMARY_LOAD_H

namespace TPCE
{

class CPGSQLHoldingSummaryLoad : public CPGSQLLoader <HOLDING_SUMMARY_ROW>
{	
private:
public:
	CPGSQLHoldingSummaryLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "HOLDING_SUMMARY")
		: CPGSQLLoader<HOLDING_SUMMARY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.HS_CA_ID));	
			buf.push_back(m_row.HS_S_SYMB);
			buf.push_back(stringify(m_row.HS_QTY));
	
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

#endif //PGSQL_HOLDING_SUMMARY_LOAD_H
