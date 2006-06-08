/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for CUSTOMER_TAXRATE table.
*/
#ifndef PGSQL_CUSTOMER_TAXRATE_LOAD_H
#define PGSQL_CUSTOMER_TAXRATE_LOAD_H

namespace TPCE
{

class CPGSQLCustomerTaxRateLoad : public CPGSQLLoader <CUSTOMER_TAXRATE_ROW>
{	
public:
	CPGSQLCustomerTaxRateLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CUSTOMER_TAXRATE")
		: CPGSQLLoader<CUSTOMER_TAXRATE_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(m_row.CX_TX_ID);
			buf.push_back(stringify(m_row.CX_C_ID));

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

#endif //PGSQL_CUSTOMER_TAXRATE_LOAD_H
