/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 August 2006
 */

#include <unistd.h>
#include <sys/syscall.h>

#include "BaseInterface.h"
#include "DBT5Consts.h"

CBaseInterface::CBaseInterface(const char type[3], char *outputDirectory,
		char *addr, const int iListenPort)
: m_szBHAddress(addr), m_iBHlistenPort(iListenPort)
{
	m_pid = syscall(SYS_gettid);

	sock = new CSocket(m_szBHAddress, m_iBHlistenPort);
	biConnect();

	char filename[iMaxPath + 1];

	memset(filename, 0, sizeof(filename));
	snprintf(filename, iMaxPath, "%s/mix-%s-%d.log", outputDirectory, type,
			m_pid);
	m_fMix.open(filename, ios::out);

	memset(filename, 0, sizeof(filename));
	snprintf(filename, iMaxPath, "%s/error-%s-%d.log", outputDirectory, type,
			m_pid);
	m_fLog.open(filename, ios::out);
}

// destructor
CBaseInterface::~CBaseInterface()
{
	biDisconnect();
	delete sock;

	m_fMix.close();
}

// connect to BrokerageHouse
bool
CBaseInterface::biConnect()
{
	try {
		sock->dbt5Connect();
		return true;
	} catch (std::runtime_error &err) {
		logErrorMessage(err.what());
		return false;
	} catch (CSocketErr *pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText()
			  << " at CBaseInterface::talkToSUT " << endl;
		logErrorMessage(osErr.str());
		return false;
	}
}

// close connection to BrokerageHouse
bool
CBaseInterface::biDisconnect()
{
	try {
		sock->dbt5Disconnect();
		return true;
	} catch (CSocketErr *pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText()
			  << " at CBaseInterface::talkToSUT " << endl;
		logErrorMessage(osErr.str());
		return false;
	}
}

// Connect to BrokerageHouse, send request, receive reply, and calculate RT
bool
CBaseInterface::talkToSUT(PMsgDriverBrokerage pRequest)
{
	int length = 0;
	TMsgBrokerageDriver Reply; // reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply));

	// record txn start time -- please, see TPC-E specification clause
	// 6.2.1.3
	CDateTime StartTime; // to time the transaction

	// send and wait for response
	try {
		length = sock->dbt5Send(
				reinterpret_cast<void *>(pRequest), sizeof(*pRequest));
	} catch (CSocketErr *pErr) {
		sock->dbt5Reconnect();
		logResponseTime(-1, 0, -1);

		ostringstream msg;
		msg << time(NULL) << " " << m_pid << " "
			<< szTransactionName[pRequest->TxnType] << ": " << endl
			<< "Error sending " << length << " bytes of data" << endl
			<< pErr->ErrorText() << endl;
		logErrorMessage(msg.str());
		length = -1;
		delete pErr;
	}
	try {
		length = sock->dbt5Receive(
				reinterpret_cast<void *>(&Reply), sizeof(Reply));
	} catch (CSocketErr *pErr) {
		logResponseTime(-1, 0, -2);

		ostringstream msg;
		msg << time(NULL) << " " << m_pid << " "
			<< szTransactionName[pRequest->TxnType] << ": " << endl
			<< "Error receiving " << length << " bytes of data" << endl
			<< pErr->ErrorText() << endl;
		logErrorMessage(msg.str());
		length = -1;
		if (pErr->getAction() == CSocketErr::ERR_SOCKET_CLOSED)
			sock->dbt5Reconnect();
		delete pErr;
	}

	// record txn end time
	CDateTime EndTime;

	// calculate txn response time
	CDateTime TxnTime;
	TxnTime.Set(0); // clear time
	TxnTime.Add(0, (int) ((EndTime - StartTime) * MsPerSecond)); // add ms

	// log response time
	logResponseTime(Reply.iStatus, pRequest->TxnType, TxnTime.MSec() / 1000.0);

	if (Reply.iStatus == CBaseTxnErr::SUCCESS)
		return true;
	return false;
}

// Log Transaction Response Times
void
CBaseInterface::logResponseTime(int iStatus, int iTxnType, double dRT)
{
	m_fMix << (long long) time(NULL) << "," << iTxnType << "," << iStatus
		   << "," << dRT << "," << m_pid << endl;
	m_fMix.flush();
}

// logErrorMessage
void
CBaseInterface::logErrorMessage(const string sErr)
{
	cerr << sErr;
	m_fLog << sErr;
	m_fLog.flush();
}

void
CBaseInterface::logStopTime()
{
	m_fMix << time(NULL) << ",STOP,,," << m_pid << endl;
	m_fMix.flush();
}
