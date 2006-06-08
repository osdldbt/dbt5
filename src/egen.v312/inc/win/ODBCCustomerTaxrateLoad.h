/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for CUSTOMER_TAXRATE table.
*/
#ifndef ODBC_CUSTOMER_TAXRATE_LOAD_H
#define ODBC_CUSTOMER_TAXRATE_LOAD_H

namespace TPCE
{

class CODBCCustomerTaxRateLoad : public CDBLoader <CUSTOMER_TAXRATE_ROW>
{	
public:
	CODBCCustomerTaxRateLoad(char *szServer, char *szDatabase, char *szTable = "CUSTOMER_TAXRATE")
		: CDBLoader<CUSTOMER_TAXRATE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CX_C_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CX_TX_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (CX_C_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_CUSTOMER_TAXRATE_LOAD_H