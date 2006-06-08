/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for CHARGE table.
*/
#ifndef PGSQL_CHARGE_LOAD_H
#define PGSQL_CHARGE_LOAD_H

namespace TPCE
{

class CPGSQLChargeLoad : public CPGSQLLoader <CHARGE_ROW>
{	
public:
	CPGSQLChargeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CHARGE")
		: CPGSQLLoader<CHARGE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.CH_TT_ID);
			buf.push_back(stringify(m_row.CH_C_TIER));
			buf.push_back(stringify(m_row.CH_CHRG));
	
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

#endif //PGSQL_CHARGE_LOAD_H
