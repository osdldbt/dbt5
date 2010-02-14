/*
 * TxnBaseDB.h
 * Base class for transacation classes
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */

#ifndef TXN_BASE_DB_H
#define TXN_BASE_DB_H

#include "locking.h"

using namespace TPCE;

class CTxnBaseDB
{

protected:
	CDBConnection*		m_pDBConnection;
	connection*		m_Conn;		// libpqxx Connection
	nontransaction*		m_Txn;		// libpqxx dummy Transaction
	CMutex		m_coutLock;

public:
	CTxnBaseDB(CDBConnection *pDBConn);
	~CTxnBaseDB() {};

	void BeginTxn();
	void CommitTxn();
	void RollbackTxn();
};

// String Tokenizer
// FIXME: This only works reliably on arrays that are not strings.
void inline Tokenize(const string& str2, vector<string>& tokens,
		const string& delimiters = ",")
{
	// This is essentially an empty array. i.e. '{}'
	if (str2.size() < 3)
		return;

	// We only call this function because we need to chop up arrays that are in
	// the format '{1,2,3,...}', so trim off the braces.  The '- 2' at the end
	// includes the string terminator and the right brace (}).
	string str = str2.substr(1, str2.size() - 2);

	// Skip delimiters at beginning.
	string::size_type lastPos = str.find_first_not_of(delimiters, 0);
	// Find first "non-delimiter".
	string::size_type pos = str.find_first_of(delimiters, lastPos);
	
	while (string::npos != pos || string::npos != lastPos)
	{
		// Found a token, add it to the vector.
		tokens.push_back(str.substr(lastPos, pos - lastPos));
		// Skip delimiters.  Note the "not_of"
		lastPos = str.find_first_not_of(delimiters, pos);
		// Find next "non-delimiter"
		pos = str.find_first_of(delimiters, lastPos);
	}
}

// This tokenizer is designed to work on arrays with strings that are
// encapsulated by double quotation marks.
// FIXME: This will fail miserably if there is an excaped double quoteation (")
// in the string.
void inline TokenizeString(const string& str2, vector<string>& tokens,
		const string& delimiters = "\"")
{
	// This is essentially an empty array. i.e. '{}'
	if (str2.size() < 3)
		return;

	// We only call this function because we need to chop up arrays that are in
	// the format '{"a","1,2,3","b"}', so trim off the braces.  The '- 2' at
	// the end includes the string terminator and the right brace (}).
	string str = str2.substr(1, str2.size() - 2);

	// Skip delimiters at beginning.
	string::size_type lastPos = str.find_first_not_of(delimiters, 0);
	// Find first "non-delimiter".
	string::size_type pos = str.find_first_of(delimiters, lastPos);

	while (string::npos != pos || string::npos != lastPos)
	{
		// Found a token, add it to the vector.
		tokens.push_back(str.substr(lastPos, pos - lastPos));

		// Skip comma separated strings.
		lastPos = str.find_first_not_of(delimiters, pos);
		pos = str.find_first_of(delimiters, lastPos);

		// Find next "non-delimiter"
		lastPos = str.find_first_not_of(delimiters, pos);
		pos = str.find_first_of(delimiters, lastPos);
	}
}

#endif	//TXN_BASE_DB_H
