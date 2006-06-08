/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing database bulk loader.
*/
#ifndef DB_LOADER_H
#define DB_LOADER_H

#include "ODBCLoad_stdafx.h"

namespace TPCE
{

// Database binding type for integer identifiers (IDENT_T metatype in TPC-E spec).
//
#define IDENT_BIND	SQLINT8

//For nullable columns
typedef struct BCPDOUBLE
{
	int		iIndicator;
	float	value;
} *PBCPDOUBLE;
typedef struct BCPDBDATETIME
{
	int			iIndicator;
	DBDATETIME	value;
} *PBCPDBDATETIME;

/*
*	DBLoader class.
*/
template <typename T> class CDBLoader : public CBaseLoader<T>
{		
protected:
	T				m_row;
	int				m_cnt;
	SQLHENV			m_henv;				// ODBC environment handle
	SQLHDBC			m_hdbc;
	SQLHSTMT		m_hstmt;			// the current hstmt
	char			m_szServer[iMaxHostname];	// server name
	char			m_szDatabase[iMaxDBName];	// name of the database being loaded
	char			m_szTable[iMaxPath];		// name of the table being loaded

//public:
	//typedef const T*	PT;		//pointer to the table row


//protected:
	virtual inline void CopyRow(PT row) { memcpy(&m_row, row, sizeof(m_row)); };

public:	

	CDBLoader(char *szServer, char *szDatabase, char *szTable);
	~CDBLoader(void);

	virtual void BindColumns() = 0; //column binding function subclasses must implement
	virtual void Init();		//resets to clean state; needed after FinishLoad to continue loading
	virtual void Commit();		// commit rows sent so far
	virtual void FinishLoad();	// finish load 
	void Connect();		//connect to SQL Server
	void Disconnect();	//disconnect - should not throw any exceptions (to put into the destructor)

	void ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType = 0, SQLHANDLE Handle = SQL_NULL_HANDLE);
	virtual void WriteNextRecord(PT next_record);
};

}	// namespace TPCE

#endif //DB_LOADER_H