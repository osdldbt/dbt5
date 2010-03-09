/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
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

#include <CThreadErr.h>

class CSocket
{
public:
	CSocket(void);
	CSocket(char* address, int port);
	~CSocket();

	int Accept(void);
	void Connect();
	int Receive(void* data, int length);
	int Send(void *data, int length);
	void Listen(const int port);

	void SetSocketfd(int sockfd) { m_sockfd = sockfd; }
	int  GetSocketfd() { return m_sockfd; }
	void CloseAccSocket() { close(m_sockfd); m_sockfd = 0; }
	void CloseListenSocket() { close(m_sockfd); }

private:
	void ThrowError(CSocketErr::Action eAction);
	int ResolveProto(const char *proto);

	char address[1024];
	int port;

	int m_listenfd;		// listen socket
	int m_sockfd;		// accept socket
};

#endif	//SOCKET_H
