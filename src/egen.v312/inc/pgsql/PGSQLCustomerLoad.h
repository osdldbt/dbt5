/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class fot this table.
*/
#ifndef PGSQL_CUSTOMER_LOAD_H
#define PGSQL_CUSTOMER_LOAD_H

namespace TPCE
{

class CPGSQLCustomerLoad : public CPGSQLLoader <CUSTOMER_ROW>
{

public:
	CPGSQLCustomerLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CUSTOMER")
		: CPGSQLLoader<CUSTOMER_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.C_ID));			
			buf.push_back(m_row.C_TAX_ID);
			buf.push_back(m_row.C_ST_ID);
			buf.push_back(m_row.C_L_NAME);
			buf.push_back(m_row.C_F_NAME);
			buf.push_back(m_row.C_M_NAME);
			buf.push_back(stringify(m_row.C_GNDR));
			buf.push_back(stringify((int)m_row.C_TIER));
			buf.push_back(m_row.C_DOB.ToStr(iDateTimeFmt));
			buf.push_back(stringify(m_row.C_AD_ID));			
			buf.push_back(m_row.C_CTRY_1);
			buf.push_back(m_row.C_AREA_1);
			buf.push_back(m_row.C_LOCAL_1);
			buf.push_back(m_row.C_EXT_1);
			buf.push_back(m_row.C_CTRY_2);
			buf.push_back(m_row.C_AREA_2);
			buf.push_back(m_row.C_LOCAL_2);
			buf.push_back(m_row.C_EXT_2);
			buf.push_back(m_row.C_CTRY_3);
			buf.push_back(m_row.C_AREA_3);
			buf.push_back(m_row.C_LOCAL_3);
			buf.push_back(m_row.C_EXT_3);
			buf.push_back(m_row.C_EMAIL_1);
			buf.push_back(m_row.C_EMAIL_2);
			
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

#endif //PGSQL_CUSTOMER_LOAD_H
