/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * Socket class for C++ (based on dbt2 _socket)
 * 25 June 2006
 */

#ifndef SOCKET_H
#define SOCKET_H

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include "CThreadErr.h"
#include "MiscConsts.h"

class CSocket
{
public:
	CSocket(void);
	CSocket(char *, int);
	~CSocket();

	int dbt5Accept(void);
	void dbt5Connect();
	int dbt5Receive(void *, int);
	int dbt5Send(void *, int);
	void dbt5Listen(const int);

	void setSocketFd(int sockfd) { m_sockfd = sockfd; }
	int getSocketFd() { return m_sockfd; }
	void closeAccSocket() { close(m_sockfd); m_sockfd = 0; }
	void closeListenSocket() { close(m_sockfd); }

private:
	void throwError(CSocketErr::Action);
	int resolveProto(const char *);

	char address[iMaxHostname + 1];
	int port;

	int m_listenfd; // listen socket
	int m_sockfd; // accept socket
};

#endif // SOCKET_H
