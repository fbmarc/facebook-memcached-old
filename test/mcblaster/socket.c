/*
** SOCKET.C
**
** Written by Steven Grimm (koreth@ebay.sun.com) on 11-26-87
** Please distribute widely, but leave my name here.
**
** Various black-box routines for socket manipulation, so I don't have to
** remember all the structure elements.
** Of course, I still have to remember how to call these routines.
*/

#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
/*#include <malloc.h>*/
#include <poll.h>

#ifndef FD_SET		/* for 4.2BSD */
#define FD_SETSIZE      (sizeof(fd_set) * 8)
#define FD_SET(n, p)    (((fd_set *) (p))->fds_bits[0] |= (1 << ((n) % 32)))
#define FD_CLR(n, p)    (((fd_set *) (p))->fds_bits[0] &= ~(1 << ((n) % 32)))
#define FD_ISSET(n, p)  (((fd_set *) (p))->fds_bits[0] & (1 << ((n) % 32)))
#define FD_ZERO(p)      bzero((char *)(p), sizeof(*(p)))
#endif

extern int errno;

#define SYSERROR perror

/*
** newsocket()
**
** Creates an Internet stream socket.
**
** Output: file descriptor of socket, or a negative error
*/
int newsocket(void)
{
	return socket(AF_INET, SOCK_STREAM, 0);
}

/*
** serversock()
**
** Creates an internet socket, binds it to an address, and prepares it for
** subsequent accept() calls by calling listen().
**
** Input: port number desired, or 0 for a random one
** Output: file descriptor of socket, or a negative error
*/
int serversock(port)
int port;
{
	int	one = 1;
	int	sock, x;
	struct	sockaddr_in server;
	int	sendBufSize = 4194304;
	int	recvBufSize = 1048576;

	sock = newsocket();
	if (sock < 0)
		return -errno;

	bzero(&server, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(port);

	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)))
		SYSERROR("setsockopt");

	if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &recvBufSize, sizeof(recvBufSize)))
		SYSERROR("setsockopt : SO_RCVBUF");

	if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sendBufSize, sizeof(sendBufSize)))
		SYSERROR("setsockopt : SO_SNDBUF");

	x = bind(sock, (struct sockaddr *)&server, sizeof(server));
	if (x < 0)
	{
		close(sock);
		return -errno;
	}

	listen(sock, 500);

	return sock;
}

/*
** portnum()
**
** Returns the internet port number for a socket.
**
** Input: file descriptor of socket
** Output: inet port number
*/
int portnum(fd)
int fd;
{
	int	err;
	socklen_t length;
	struct	sockaddr_in address;

	length = sizeof(address);
	err = getsockname(fd, (struct sockaddr *)&address, &length);
	if (err < 0)
		return -errno;

	return ntohs(address.sin_port);
}

/*
** host_to_addr()
**
** Places in _buf_ the IP address of a host in human-readable dotted-quad
** format. Returns the address on success, 0 if lookup failed.
**
** Input: hostname to look up
**	  buffer for IP address
*/
in_addr_t host_to_addr(const char *hostname, char *buf)
{
	struct hostent *hp, *gethostbyname();
	struct in_addr addr;

	if (isdigit(hostname[0])) {
                if (inet_aton(hostname, &addr)) {
                        if (buf)
                               strcpy(buf, hostname);
                        return addr.s_addr;
                }
                return 0;

        }
	else
	{
		hp = gethostbyname(hostname);
		if (hp == NULL) {
			*buf = '\0';
                        return 0;
                }
		else
		{
			memcpy(&addr, hp->h_addr, hp->h_length);
                        if (buf)
			       strcpy(buf, inet_ntoa(addr));
                        return addr.s_addr;
		}
	}
}

/*
** clientsock()
**
** Returns a connected client socket.
**
** Input: host name and port number to connect to
** Output: file descriptor of CONNECTED socket, or a negative error
** (-9999) if the hostname was bad.
*/
int clientsock(host, port)
char *host;
int port;
{
	int	sock;
	struct	sockaddr_in server;
	struct	hostent *hp, *gethostbyname();

	bzero(&server, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	if (isdigit(host[0]))
		server.sin_addr.s_addr = inet_addr(host);
	else
	{
		hp = gethostbyname(host);
		if (hp == NULL)
			return -9999;
		bcopy(hp->h_addr, &server.sin_addr, hp->h_length);
	}

	sock = newsocket();
	if (sock < 0)
		return -errno;

	if (connect(sock, (struct sockaddr *)&server, sizeof(server)) < 0)
	{
		close(sock);
		return -errno;
	}

	return sock;
}

/*
** clientsock_nb()
**
** Returns a nonblocking client socket with a pending connection attempt.
**
** Input: host name to connect to
**        port number to connect to
**        buffer for file descriptor
** Output: <0 if the connection attempt failed
**         0  if the connection is fully established (e.g. because it's a
**            connection to the local host)
**         >0 if the connection is pending
*/
int clientsock_nb(host, port, sock)
char *host;
int port;
int *sock;
{
	struct	sockaddr_in server;
	struct	hostent *hp, *gethostbyname();

	bzero(&server, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	if (isdigit(host[0]))
		server.sin_addr.s_addr = inet_addr(host);
	else
	{
		hp = gethostbyname(host);
		if (hp == NULL)
			return -9999;
		bcopy(hp->h_addr, &server.sin_addr, hp->h_length);
	}

	*sock = newsocket();
	if (*sock < 0)
		return -1;

	if (fcntl(*sock, F_SETFL, O_NONBLOCK))
		return -1;

	if (connect(*sock, (struct sockaddr *)&server, sizeof(server)) < 0)
	{
		if (errno != EINPROGRESS)
		{
			close(*sock);
			return -errno;
		}
	}
	else
		return 0;

	return 1;
}

/*
** readable()
**
** Poll a socket for pending input.  Returns immediately.  This is a front-end
** to waitread() below.
**
** Input: file descriptor to poll
** Output: 1 if data is available for reading
*/
int readable(fd)
int fd;
{
	return(waitread(fd, 0));
}

/*
** waitpoll()
**
** Wait for a condition on a file descriptor for a little while.
**
** Input: file descriptor to watch
**        condition to wait for
**	  how long to wait, in seconds, before returning
** Output: 1 if the descriptor is writable
**	   0 if the timer expired or a signal occurred.
*/
int waitpoll(fd, cond, time)
int fd, cond, time;
{
	struct pollfd pfd;

	pfd.fd = fd;
	pfd.events = cond;
	pfd.revents = 0;

	switch (poll(&pfd, 1, time * 1000)) {
	case 1:
		return 1;
	case 0:
		return 0;
	default:
		/* an error! */
		return 0;
	}
}

/*
** waitread()
**
** Wait for data on a file descriptor for a little while.
**
** Input: file descriptor to watch
**	  how long to wait, in seconds, before returning
** Output: 1 if data was available
**	   0 if the timer expired or a signal occurred.
*/
int waitread(fd, time)
int fd, time;
{
	return waitpoll(fd, POLLIN, time);
}

/*
** waitwrite()
**
** Wait for writability on a file descriptor for a little while.
**
** Input: file descriptor to watch
**	  how long to wait, in seconds, before returning
** Output: 1 if the descriptor is writable
**	   0 if the timer expired or a signal occurred.
*/
int waitwrite(fd, time)
int fd, time;
{
	return waitpoll(fd, POLLOUT, time);
}

/*
** set_nodelay()
**
** Set or clear the TCP_NODELAY option on a file descriptor.
**
** Input: file descriptor to modify
**        whether NODELAY is desired
*/
int set_nodelay(fd, want_nodelay)
int fd, want_nodelay;
{
	return(setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *)&want_nodelay, sizeof(int)));
}


/*
** set_nonblock()
**
** Sets nonblocking mode on a file descriptor.
**
** Input: file descriptor to modify
*/
int set_nonblock(fd)
int fd;
{
	return (fcntl(fd, F_SETFL, O_NONBLOCK));
}
