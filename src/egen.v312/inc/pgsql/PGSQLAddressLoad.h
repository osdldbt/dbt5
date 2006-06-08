/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento


/*
*	Database loader class for ADDRESS table.
*/
#ifndef PGSQL_ADDRESS_LOAD_H
#define PGSQL_ADDRESS_LOAD_H

namespace TPCE
{

class CPGSQLAddressLoad : public CPGSQLLoader <ADDRESS_ROW>
{	
public:
	CPGSQLAddressLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "ADDRESS")
		: CPGSQLLoader<ADDRESS_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.AD_ID));			
			buf.push_back(m_row.AD_LINE1);
			buf.push_back(m_row.AD_LINE2);
			buf.push_back(m_row.AD_ZC_CODE);
			buf.push_back(m_row.AD_CTRY);
	
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

#endif //PGSQL_ADDRESS_LOAD_H
