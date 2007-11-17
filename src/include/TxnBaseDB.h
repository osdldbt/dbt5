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

namespace TPCE
{

class CTxnBaseDB
{

protected:
	CDBConnection*		m_pDBConnection;
	connection*		m_Conn;		// libpqxx Connection
	nontransaction*		m_Txn;		// libpqxx dummy Transaction
	CSyncLock		m_coutLock;

public:
	CTxnBaseDB(CDBConnection *pDBConn);
	~CTxnBaseDB() {};

	void BeginTxn();
	void CommitTxn();
	void RollbackTxn();
};

// String Tokenizer
void inline Tokenize(const string& str2, vector<string>& tokens,
		const string& delimiters = ",")
{
	// This is essential an empty array. i.e. '{}'
	if (str2.size() < 3)
		return;

	// We only call this function because we need to chop up arrays that
	// are in the format '{1,2,3,...}', so trim off the braces.
	string str = str2.substr(1, str2.size() - 1);

	// Skip delimiters at beginning.
	string::size_type lastPos = str.find_first_not_of(delimiters, 0);
	// Find first "non-delimiter".
	string::size_type pos     = str.find_first_of(delimiters, lastPos);
	
	while (pos < str2.size())
	{
		// Found a token, add it to the vector.
		tokens.push_back(str.substr(lastPos, pos - lastPos));
		// Skip delimiters.  Note the "not_of"
		lastPos = str.find_first_not_of(delimiters, pos);
		// Find next "non-delimiter"
		pos = str.find_first_of(delimiters, lastPos);
	}
}

}	// namespace TPCE

#endif	//TXN_BASE_DB_H
