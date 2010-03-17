/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
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
	CMutex *m_pLogLock;
	CMutex *m_pMixLock;
	ofstream *m_pfLog; // error log file
	ofstream *m_pfMix; // mix log file

private:
	CSocket	*sock;
	void logResponseTime(int, int, double);
	
public:

	CBaseInterface(char *, const int, ofstream *, ofstream *, CMutex *,
			CMutex *);
	~CBaseInterface(void);
	bool biConnect();
	bool biDisconnect();
};

#endif // BASE_INTERFACE_H
