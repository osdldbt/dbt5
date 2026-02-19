/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - 2006 Rilson Nascimento
 * - 2010 Mark Wong <markwkm@postgresql.org>
 */

//
// Class representing PostgreSQL database loader.
//

#ifndef PG_LOADER_H
#define PG_LOADER_H

#include <cstring>
#include <libpq-fe.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

namespace TPCE
{
const int iDateTimeFmt = 11;
const int iConnectStrLen = 256;
const int iCopyBufSize = 131072;

//
// PGSQLLoader class.
//
template <typename T> class CPGSQLLoader: public CBaseLoader<T>
{
private:
	void Copy();

protected:
	PGconn *m_Conn;
	char m_szConnectStr[iConnectStrLen + 1];
	char m_szTable[iMaxPath + 1]; // name of the table being loaded
	char *m_szCopyBuf;

	void
	CopyRecord(const char *fmt, ...)
	{
		va_list args;
		va_start(args, fmt);
		int len = vsnprintf(m_szCopyBuf, iCopyBufSize, fmt, args);
		va_end(args);

		if (len < 0) {
			throw CSystemErr(
					CSystemErr::eWriteFile, "CPGSQLLoader::CopyRecord");
		}

		if (len >= iCopyBufSize) {
			len = iCopyBufSize - 1;
		}

		if (PQputCopyData(m_Conn, m_szCopyBuf, len) != 1) {
			cout << "PQputCopyData failed: " << PQerrorMessage(m_Conn) << endl;
			throw CSystemErr(
					CSystemErr::eWriteFile, "CPGSQLLoader::CopyRecord");
		}
	}

public:
	typedef const T *PT; // pointer to the table row

	CPGSQLLoader(const char *szConnectStr, const char *szTable);
	~CPGSQLLoader(void);

	// resets to clean state; needed after FinishLoad to continue loading
	void Init();

	void Commit(); // commit rows sent so far
	void FinishLoad(); // finish load
	void Connect(); // connect to PostgreSQL

	// disconnect - should not throw any exceptions (to put into the
	// destructor)
	void Disconnect();

	void
	WriteNextRecord(const T &next_record)
	{
		printf("pgloader - const ref\n");
	};
};

//
// The constructor.
//
template <typename T>
CPGSQLLoader<T>::CPGSQLLoader(const char *szConnectStr, const char *szTable)
: m_Conn(NULL), m_szCopyBuf(NULL)
{
	strncpy(m_szConnectStr, szConnectStr, iConnectStrLen);
	m_szConnectStr[iConnectStrLen] = '\0';
	strncpy(m_szTable, szTable, iMaxPath);
	m_szTable[iMaxPath] = '\0';

	m_szCopyBuf = (char *) malloc(iCopyBufSize);
	if (m_szCopyBuf == NULL) {
		throw CSystemErr(
				CSystemErr::eCreateFile, "CPGSQLLoader::CPGSQLLoader malloc");
	}
}

//
// Destructor closes the connection.
//
template <typename T> CPGSQLLoader<T>::~CPGSQLLoader()
{
	Disconnect();
	if (m_szCopyBuf != NULL) {
		free(m_szCopyBuf);
		m_szCopyBuf = NULL;
	}
}

//
// Reset state e.g. close the connection, bind columns again, and reopen.
// Needed after Commit() to continue loading.
//
template <typename T>
void
CPGSQLLoader<T>::Init()
{
	Connect();
}

template <typename T>
void
CPGSQLLoader<T>::Connect()
{
	m_Conn = PQconnectdb(m_szConnectStr);
	if (PQstatus(m_Conn) != CONNECTION_OK) {
		cout << "Connection to database failed: " << PQerrorMessage(m_Conn)
			 << endl;
		PQfinish(m_Conn);
		m_Conn = NULL;
		exit(1);
	}

	Copy();
}

template <typename T>
void
CPGSQLLoader<T>::Copy()
{
	PGresult *res;

	res = PQexec(m_Conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		cout << "BEGIN failed: " << PQerrorMessage(m_Conn) << endl;
		PQclear(res);
		exit(1);
	}
	PQclear(res);

	char szCopyCmd[256];
	snprintf(szCopyCmd, sizeof(szCopyCmd),
			"COPY %s FROM STDIN WITH (DELIMITER '|', NULL '')", m_szTable);

	res = PQexec(m_Conn, szCopyCmd);
	if (PQresultStatus(res) != PGRES_COPY_IN) {
		cout << "COPY command failed: " << PQerrorMessage(m_Conn) << endl;
		PQclear(res);
		exit(1);
	}
	PQclear(res);
}

//
// Commit sent rows. This needs to be called every so often to avoid row-level
// lock accumulation.
//
template <typename T>
void
CPGSQLLoader<T>::Commit()
{
	FinishLoad();
	Copy();
}

//
// Commit sent rows. This needs to be called after the last row has been sent
// and before the object is destructed. Otherwise all rows will be discarded
// since this is in a transaction.
//
template <typename T>
void
CPGSQLLoader<T>::FinishLoad()
{
	PGresult *res;

	if (PQputCopyEnd(m_Conn, NULL) != 1) {
		cout << "PQputCopyEnd failed: " << PQerrorMessage(m_Conn) << endl;
		exit(1);
	}

	res = PQgetResult(m_Conn);
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		cout << "COPY failed: " << PQerrorMessage(m_Conn) << endl;
		PQclear(res);
		exit(1);
	}
	PQclear(res);

	res = PQexec(m_Conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		cout << "COMMIT failed: " << PQerrorMessage(m_Conn) << endl;
		PQclear(res);
		exit(1);
	}
	PQclear(res);
}

//
// Disconnect from the server. Should not throw any exceptions.
//
template <typename T>
void
CPGSQLLoader<T>::Disconnect()
{
	if (m_Conn != NULL) {
		PQfinish(m_Conn);
		m_Conn = NULL;
	}
}

} // namespace TPCE

#endif // PG_LOADER_H
