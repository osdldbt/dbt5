/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Base class for emulator-SUT interface
 * 13 August 2006
 */

#ifndef BASE_INTERFACE_H
#define BASE_INTERFACE_H

#include "locking.h"

#include "CommonStructs.h"
#include "CSocket.h"
using namespace TPCE;

class CBaseInterface
{
protected:
	bool talkToSUT(PMsgDriverBrokerage);
	void logErrorMessage(const string);

	char *m_szBHAddress;
	int m_iBHlistenPort;

private:
	CSocket *sock;
	pid_t m_pid;
	ofstream m_fLog; // error log file
	ofstream m_fMix; // mix log file

	void logResponseTime(int, int, double);

public:
	CBaseInterface(const char *, char *, char *, const int);
	~CBaseInterface(void);
	bool biConnect();
	bool biDisconnect();

	void logStopTime();
};

#endif // BASE_INTERFACE_H
