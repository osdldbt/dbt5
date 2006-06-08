/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento
 
/*
*	Class representing PostgreSQL database loader.
*/
#ifndef PG_LOADER_H
#define PG_LOADER_H

#include "PGSQLLoad.h"

namespace TPCE
{

/*
*	PGSQLLoader class.
*/
template <typename T> class CPGSQLLoader : public CBaseLoader<T>
{		
protected:
	T				m_row;
	int				m_cnt;
	connection*			m_Conn;			// libpqxx Connection
	work*			m_Txn;			// libpqxx Transaction
	char				m_stmt[iMaxStmt];	// insert statement
	tablewriter*			m_TW;			// libpqxx tablewriter
	vector<string>			buf;
	
	char			m_szHost[iMaxPGHost];		// host name
	char			m_szDBName[iMaxPGDBName];	// database name
	char 			m_szPostmasterPort[iMaxPGPort]; // PostgreSQL postmaster port
	char			m_szTable[iMaxPath];		// name of the table being loaded

public:
	typedef const T*	PT;		//pointer to the table row

protected:
	virtual inline void CopyRow(PT row) { memcpy(&m_row, row, sizeof(m_row)); };

	template<typename R> inline string stringify(const R& x)	// convert to string
	{
		ostringstream o;
		if (!(o << x))	throw runtime_error(string("stringify(") + typeid(x).name() + ")");
		return o.str();
	}


public:	

	CPGSQLLoader(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable);
	~CPGSQLLoader(void);

	virtual void Init();			// resets to clean state; needed after FinishLoad to continue loading
	virtual void Commit();			// commit rows sent so far
	virtual void FinishLoad();		// finish load 
	void Connect();				// connect to PostgreSQL
	void Disconnect();			// disconnect - should not throw any exceptions (to put into the destructor)

	virtual void WriteNextRecord(PT next_record);
};

/*
*	The constructor.
*/
template <typename T>
CPGSQLLoader<T>::CPGSQLLoader(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable)
: m_cnt(0)
//, m_henv(SQL_NULL_HENV)  //FIXME: initialize variable members
//, m_hdbc(SQL_NULL_HDBC)
//, m_hstmt(SQL_NULL_HSTMT)
{	
	memset(m_szHost, 0, sizeof(m_szHost));
	strncpy(m_szHost, szHost, sizeof(m_szHost) - 1);

	memset(m_szDBName, 0, sizeof(m_szDBName));
	strncpy(m_szDBName, szDBName, sizeof(m_szDBName) - 1);

	memset(m_szPostmasterPort, 0, sizeof(m_szPostmasterPort));
	strncpy(m_szPostmasterPort, szPostmasterPort, sizeof(m_szPostmasterPort) - 1);

	memset(m_szTable, 0, sizeof(m_szTable));
	strncpy(m_szTable, szTable, sizeof(m_szTable) - 1);
}

/*
*	Destructor closes the connection.
*/
template <typename T>
CPGSQLLoader<T>::~CPGSQLLoader()
{
	Disconnect();
}

/*
*	Reset state e.g. close the connection, bind columns again, and reopen.
*	Needed after Commit() to continue loading.
*/
template <typename T>
void CPGSQLLoader<T>::Init()
{
	Connect();
}

/*
*	Create connection handles and connect to PostgreSQL
*/
template <typename T>
void CPGSQLLoader<T>::Connect()
{
	// Set up a connection to the backend

	try
	{
		char			szConnectStr[256];
		sprintf( szConnectStr, "host=%s dbname=%s port=%s", 
 				m_szHost, m_szDBName, m_szPostmasterPort );
		
		m_Conn = new connection( szConnectStr );
		m_Txn = new work( *m_Conn, "txn" );
		m_TW = new tablewriter( *m_Txn, m_szTable );
	}
	catch(const exception &e)
	{
		cerr<< "libpqxx: "<<e.what() << endl;
	}
 
	//Now prepare the insert statement. This is table-specific and must be
	//defined in subclasses.
}

/*
*	Commit sent rows. This needs to be called every so often to avoid row-level lock accumulation.
*/
template <typename T>
void CPGSQLLoader<T>::Commit()
{
	m_TW->complete();
	m_Txn->commit();

 	delete m_TW;
 	delete m_Txn;

	m_Txn = new work( *m_Conn, "txn" );
	m_TW = new tablewriter( *m_Txn, m_szTable );
}

/*
*	Commit sent rows. This needs to be called after the last row has been sent
*	and before the object is destructed. Otherwise all rows will be discarded.
*/
template <typename T>
void CPGSQLLoader<T>::FinishLoad()
{
	m_TW->complete();
	m_Txn->commit();
	delete m_TW;
	delete m_Txn;

	Disconnect();	// While destructor is not being called
}

/*
*	Disconnect from the server. Should not throw any exceptions.
*/
template <typename T>
void CPGSQLLoader<T>::Disconnect()
{
	//FIXME: Destructor is not being called when EGenGenerateAndLoad calls "delete pLoad;"
	m_Conn->disconnect();
	delete m_Conn;
}

/*
*	Loads a record into the database.
*/
template <typename T>
void CPGSQLLoader<T>::WriteNextRecord(PT next_record)
{
	CopyRow(next_record);	//copy to the bound location inside this class first

	try {
		//cout<<m_stmt<<endl; // debug
		m_Txn->exec(m_stmt);
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


}	// namespace TPCE


#endif //PG_LOADER_H
