/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for ZIP_CODE table.
*/
#ifndef PGSQL_ZIP_CODE_LOAD_H
#define PGSQL_ZIP_CODE_LOAD_H

namespace TPCE
{

class CPGSQLZipCodeLoad : public CPGSQLLoader <ZIP_CODE_ROW>
{	
public:
	CPGSQLZipCodeLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "ZIP_CODE")
		: CPGSQLLoader<ZIP_CODE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.ZC_CODE);
			buf.push_back(m_row.ZC_TOWN);
			buf.push_back(m_row.ZC_DIV);
	
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

#endif //PGSQL_ZIP_CODE_LOAD_H
