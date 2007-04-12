/*
 * CSocket.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 25 June 2006
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <CSocket.h>
#include <dbt5error.h>

#define LISTENQ     1024

using namespace TPCE;

//Constructor
CSocket::CSocket(void)
: m_listenfd(0),
  m_sockfd(0)
{
}

CSocket::CSocket(char* address, int port)
: m_listenfd(0),
  m_sockfd(0)
{
	strcpy(this->address, address);
	this->port = port;
}

//Destructor
CSocket::~CSocket()
{
	if (m_listenfd != 0) close(m_listenfd);
	if (m_sockfd != 0) close(m_sockfd);
}

// Accept
int CSocket::Accept(void)
{
	struct sockaddr_in sa;

	socklen_t addrlen = sizeof(sa);
	errno = 0;
	m_sockfd = accept(m_listenfd, (struct sockaddr *) &sa, &addrlen);
	if (m_sockfd == -1)
	{
		ThrowError(CSocketErr::ERR_SOCKET_ACCEPT);
	}
	return m_sockfd;
}

// Connect
void CSocket::Connect()
{
	errno = 0;
	m_sockfd = socket(AF_INET, SOCK_STREAM, ResolveProto("tcp"));
	if (m_sockfd == -1)
	{
		ThrowError(CSocketErr::ERR_SOCKET_CREATE);
	}

	struct sockaddr_in sa;
	bzero(&sa, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons(port);
	if (sa.sin_port == 0)
	{
		ThrowError(CSocketErr::ERR_SOCKET_SINPORT);
	}

	if ((inet_pton(AF_INET, address, &sa.sin_addr)) <= 0)
	{
		struct hostent *he;
		if ((he = gethostbyname(address)) == NULL)
		{
			ThrowError(CSocketErr::ERR_SOCKET_HOSTBYNAME);
		}
		memcpy(&sa.sin_addr, he->h_addr_list[0], he->h_length);
		//ThrowError(CSocketErr::ERR_SOCKET_INETPTON, "CSocket::Connect");
	}
	
	errno = 0;
	// Try to connect 5 times total, waiting 1 second between attempts.
	bool ok = false;
	for (int i = 0; i < 5; i++)
	{
		if ((connect(m_sockfd, (struct sockaddr *) &sa, sizeof(sa))) != -1)
		{
			ok = true;
			break;
		}
		sleep(1);
	}
	if (ok == false) {
		ThrowError(CSocketErr::ERR_SOCKET_CONNECT);
	}

}

// Receive
void CSocket::Receive(void* data, int length)
{
	int received, total, remaining;
	remaining = length;
	total = 0;
	char* szData = NULL;
	do {
		errno = 0;
		received = recv(m_sockfd, data, remaining, 0);
		if (received == -1)
		{
			ThrowError(CSocketErr::ERR_SOCKET_RECV);
		}
		else if (received == 0) 
		{
			ThrowError(CSocketErr::ERR_SOCKET_CLOSED);
		}

		total += received;
		szData = reinterpret_cast<char*>(data);

		szData += received;
		//data += received; // C++ doesn't like arithmetic with void*
		data = reinterpret_cast<void*>(szData);
		remaining -= received;
	}
	while (total != length);

	if (length != total)
	{
		ThrowError(CSocketErr::ERR_SOCKET_RECVPARTIAL);
	}

	//return total;
}

// Send
void CSocket::Send(void* data, int length)
{
	int sent = 0;
	int remaining = length;
	char* szData = NULL;
	do {
		errno = 0;
		sent = send(m_sockfd, (void*)data, remaining, 0);

		if (sent == -1) 
		{
			ThrowError(CSocketErr::ERR_SOCKET_SEND);
		}
		else if (sent == 0)
		{
			ThrowError(CSocketErr::ERR_SOCKET_CLOSED);
		}

		szData = reinterpret_cast<char*>(data);
		szData += sent;
		data = reinterpret_cast<void*>(szData);
		//data += sent; // C++ doesn't like arithmetic with void*
		remaining -= sent;
	} while (sent != length);

	if (length != sent)
	{
		ThrowError(CSocketErr::ERR_SOCKET_SENDPARTIAL);
	}

	//return sent;
}

// Listen
void CSocket::Listen(const int port)
{
	struct sockaddr_in sa;
	
	errno = 0;
	m_listenfd = socket(AF_INET, SOCK_STREAM, ResolveProto("tcp"));
	if (m_listenfd < 0)
	{
		ThrowError(CSocketErr::ERR_SOCKET_CREATE);
		//perror("_listen");
	}

	bzero(&sa, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_ANY);
	sa.sin_port = htons(port);
	
	errno = 0;
	if (bind(m_listenfd, (struct sockaddr *) &sa, sizeof(sa)) < 0)
	{
		ThrowError(CSocketErr::ERR_SOCKET_BIND);
		//perror("_listen");
	}

	errno = 0;	
	if (listen(m_listenfd, LISTENQ) < 0)
	{
		ThrowError(CSocketErr::ERR_SOCKET_LISTEN);
                //perror("_listen");
	}

}

// ResolveProto
int CSocket::ResolveProto(const char *proto)
{
	struct protoent *protocol;

	protocol = getprotobyname(proto);
	if (!protocol)
	{
		ThrowError(CSocketErr::ERR_SOCKET_RESOLVPROTO);
	}

	return protocol->p_proto;
}

// ThrowError
void CSocket::ThrowError(CSocketErr::Action eAction)
{
	throw new CSocketErr( eAction );
}
