/*
 * Copyright (C) 2010-2015 Mail.RU
 * Copyright (C) 2012 Teodor Sigaev
 * Copyright (C) 2010-2015 Yury Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#ifdef LIBIPROTO_OCTOPUS
#include <config.h>
#endif
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <sys/param.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>

#ifdef LIBIPROTO_OCTOPUS
#include <client/libiproto/libiproto.h>
#include <third_party/queue.h>
#else
#include <libiproto.h>
#include <queue.h>
#endif
#include <iproto_def.h>

#define SUM_ERROR_CODES(x) LIBIPROTO_ERROR_CODES(x) ERROR_CODES(x)
enum li_error_codes ENUM_INITIALIZER(SUM_ERROR_CODES);

#ifndef offsetof
#define offsetof(type, field)   ((int) (uintptr_t)&((type *)0)->field)
#endif   /* offsetof */

#define TIMESPEC_DIFF(tp1_, tp2_) (			\
	((tp1_).tv_sec - (tp2_).tv_sec) * 1000 +	\
	((tp1_).tv_nsec - (tp2_).tv_nsec) / 1000000	\
)

#define MH_STATIC
#define mh_name _sp_request
#define mh_key_t u_int32_t
#define mh_val_t struct iproto_request_t*
#include <mhash.h>


struct memory_arena_t {
	u_int32_t			refCounter;
	struct	memory_arena_pool_t	*rap;
	TAILQ_ENTRY(memory_arena_t)	link;
	size_t				arenaSize;
	size_t				arenaEnd;
	size_t				arenaBegin;
	char				data[0];
};

#define MEMORYARENAHDRZ	(offsetof(struct memory_arena_t, data))

struct memory_arena_pool_t {
	u_int32_t				maxArenas;
	u_int32_t				nArenas;
	size_t					memoryArenaSize;
	memalloc				sp_alloc;

	TAILQ_HEAD(freelist, memory_arena_t) 	freelist;
	TAILQ_HEAD(lockedlist, memory_arena_t)	lockedlist;
};

struct memory_arena_pool_t*
map_alloc(memalloc sp_alloc, u_int32_t maxArenas, size_t memoryArenaSize) {
	struct memory_arena_pool_t	*rap = sp_alloc(NULL, sizeof(*rap));

	if (!rap)
		return NULL;
	memset(rap, 0, sizeof(*rap));

	rap->sp_alloc = sp_alloc;
	rap->maxArenas = maxArenas;
	rap->memoryArenaSize = memoryArenaSize;

	TAILQ_INIT(&rap->freelist);
	TAILQ_INIT(&rap->lockedlist);

	return rap;
}

static inline void
memory_arena_incr_refcount(struct memory_arena_t *arena) {
	arena->refCounter++;
}

static struct memory_arena_t*
map_get_arena(struct memory_arena_pool_t* rap, size_t size) {
	struct memory_arena_t *arena = NULL;

	size = MAX(rap->memoryArenaSize, size);

	if (!TAILQ_EMPTY(&rap->freelist)) {
		TAILQ_FOREACH(arena, &rap->freelist, link) {
			assert(arena->rap == rap);
			assert(arena->refCounter == 1);
			if (arena->arenaSize >= size) {
				TAILQ_REMOVE(&rap->freelist, arena, link);
				break;
			}
		}
	}

	if (!arena) {
		rap->nArenas++;
		arena = rap->sp_alloc(NULL, size + MEMORYARENAHDRZ);
		arena->arenaSize = size;
		arena->rap = rap;
		arena->refCounter = 1;
	}

	arena->arenaBegin = arena->arenaEnd = 0;
	TAILQ_INSERT_TAIL(&rap->lockedlist, arena, link);

	return arena;
}

static void
memory_arena_decr_refcount(struct memory_arena_t *arena) {
	struct memory_arena_pool_t* rap = arena->rap;

	assert(arena->refCounter > 0);
	arena->refCounter--;

	if (arena->refCounter == 0) {
		arena->rap->sp_alloc(arena, 0);
	} else if (arena->refCounter == 1 && rap != NULL) {
		TAILQ_REMOVE(&rap->lockedlist, arena, link);

		if (rap->nArenas > rap->maxArenas) {
			arena->rap->sp_alloc(arena, 0);
			rap->nArenas--;
		} else {
			TAILQ_INSERT_TAIL(&rap->freelist, arena, link);
		}
	}
}

void
map_free(struct memory_arena_pool_t *rap) {
	struct memory_arena_t	*arena, *tmp;

	TAILQ_FOREACH_SAFE(arena, &rap->freelist, link, tmp) {
		assert(arena->refCounter == 1);
		arena->rap->sp_alloc(arena, 0);
	}

	TAILQ_FOREACH_SAFE(arena, &rap->lockedlist, link, tmp) {
		assert(arena->refCounter > 1);
		arena->rap = NULL;
		arena->refCounter--;
	}

	rap->sp_alloc(rap, 0);
}

struct iproto_connection_t {
	int					fd;
	bool					nonblock;
	bool					has4errcode;
	enum {
			NotConnected = 0,
			ConnectInProgress,
			Connected,
			ConnectionError
	}					connectState;
	enum {
			TcpFamily = 0,
			UnixFamily
	}					connectFamily;
	union {
		struct {
			struct sockaddr_in	addr;
			char			*server;
			int			port;
		} inet;
		struct {
			struct sockaddr_un	addr;
			char			*path;
		} un;
	}					serv_addr;
	memalloc				sp_alloc;
	struct mh_sp_request_t			*requestHash;
	u_int32_t				mirrorCnt;
	u_int32_t				nReqInProgress;

	TAILQ_HEAD(sendlist, iproto_request_t) 	sendList;
	TAILQ_HEAD(recvlist, iproto_request_t) 	recvList;

	struct memory_arena_pool_t		*reqap;
	struct memory_arena_t			*reqArena;

	struct memory_arena_pool_t		*rap;
	struct memory_arena_t			*readArena;
#define	MINNEEDEDSIZE		(8 * sizeof(struct iproto_retcode))
	size_t					neededSize;

	struct iovec				*iovptr,
						*iovSend;
	int					iovSendLength,
						iovSendLengthMax;
};

struct iproto_request_t {
	struct iproto_connection_t	*c;
	u_int32_t			state;
	bool				dataResponsibility;
	struct memory_arena_t		*reqArena;
	struct memory_arena_t		*dataArena;

	union iproto_any_header		*headerRecv;
	char				*dataRecv;

	struct memory_arena_t		*readArena;

	TAILQ_ENTRY(iproto_request_t)   link;
	void				*assocData;

	size_t				dataSendSize;
	char				*dataSend;
	struct iproto			headerSend; /* clang wants it at the end */
};

struct iproto_connection_t*
li_conn_init(memalloc sp_alloc, struct memory_arena_pool_t *rap, struct memory_arena_pool_t *reqap) {
	struct iproto_connection_t*	c;

	c = sp_alloc(NULL, sizeof(*c));

	memset(c, 0, sizeof(*c));

	c->fd = -1;
	c->sp_alloc = sp_alloc;
	c->rap = rap;
	c->reqap = reqap;
	c->requestHash = mh_sp_request_init(sp_alloc);
	c->readArena = NULL;
	c->neededSize = MINNEEDEDSIZE;

	TAILQ_INIT(&c->sendList);
	TAILQ_INIT(&c->recvList);

	return c;
}

static int
libpoll(int fd, int event, u_int32_t timeout) {
	struct pollfd	pfd;
	int		ret;

	pfd.fd = fd;
	pfd.events = event;
	pfd.revents = 0;

	ret = poll( &pfd, 1, timeout);
	if (ret < 0 || (pfd.revents & (POLLHUP | POLLNVAL | POLLERR)) != 0)
		return (pfd.revents | POLLERR);

	return pfd.revents;
}

static u_int32_t
li_connect_phase1(struct iproto_connection_t *c, u_int32_t timeout) {
	u_int32_t	r;

	if (c->fd < 0)
		return ERR_CODE_OK;

	switch(c->connectState) {
		case Connected:
			return ERR_CODE_ALREADY_CONNECTED;
		case ConnectionError:
			return ERR_CODE_CONNECT_ERR;
		case ConnectInProgress:
			r = libpoll(c->fd, POLLOUT, timeout);

			if (r & POLLERR) {
				c->connectState = ConnectionError;
				return ERR_CODE_CONNECT_ERR;
			}

			if (r & POLLOUT) {
				int err;
				socklen_t err_size = sizeof(err);
				getsockopt(c->fd, SOL_SOCKET, SO_ERROR,
					   &err, &err_size);

				if (err != 0) {
					errno = err;
					c->connectState = ConnectionError;
					return ERR_CODE_CONNECT_ERR;
				}

				c->connectState = Connected;

				return ERR_CODE_OK;
			}

			return ERR_CODE_CONNECT_IN_PROGRESS;

		case NotConnected:
		default:
			abort();
	}

	return  ERR_CODE_OK;
}

static u_int32_t
li_connect_phase2(struct iproto_connection_t *c, u_int32_t opt) {
	int		flags;
	struct sockaddr	*addr;
	socklen_t	addr_len;

	c->nonblock = (opt & LIBIPROTO_OPT_NONBLOCK) ? true : false;
	c->has4errcode = (opt & LIBIPROTO_OPT_HAS_4BYTE_ERRCODE) ? true : false;

	if (c->nonblock && ((flags=fcntl(c->fd,F_GETFL,0)) == -1 || fcntl(c->fd,F_SETFL,flags|O_NDELAY) < 0)) {
		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

	switch (c->connectFamily) {
	case TcpFamily:
		addr = (struct sockaddr *)&c->serv_addr.inet.addr;
		addr_len = sizeof(c->serv_addr.inet.addr);
		break;
	case UnixFamily:
		addr = (struct sockaddr *)&c->serv_addr.un.addr;
		addr_len = sizeof(c->serv_addr.un.addr.sun_family) + strlen(c->serv_addr.un.addr.sun_path);
		break;
	default:
		abort();
	}


	if (opt & LIBIPROTO_OPT_HELLO) {
		c->iovSendLengthMax = 2;
		c->iovSend = c->iovptr = c->sp_alloc(c->iovSend, c->iovSendLengthMax * sizeof(struct iovec));
		c->iovSendLength = 0;
		static char hello_body[] = {0x20,0x00,0x00,0x00,0x05,0x00,0x00,0x00,0x74,0x65,0x73,0x74,0x73,
					    0x09,0x00,0x00,0x00,0x6c,0x6f,0x63,0x61,0x6c,0x68,0x6f,0x73,0x74,
					    0x06,0x00,0x00,0x00,0x6e,0x6f,0x63,0x6f,0x6e,0x66};
		static struct iproto msg = { .msg_code = 0xff06,
					     .data_len = sizeof(hello_body) };
		c->iovSend[c->iovSendLength].iov_base = &msg;
		c->iovSend[c->iovSendLength].iov_len = sizeof(msg);
		c->iovSendLength++;
		c->iovSend[c->iovSendLength].iov_base = hello_body;
		c->iovSend[c->iovSendLength].iov_len = sizeof(hello_body);
		c->iovSendLength++;
	}

	if ( connect(c->fd, addr, addr_len) < 0 ) {
		if ( errno == EINPROGRESS || errno == EALREADY )
			return ERR_CODE_CONNECT_IN_PROGRESS;

		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

	c->connectState = Connected;
	return ERR_CODE_OK;
}

u_int32_t
li_connect(struct iproto_connection_t *c, const char *server, int port, u_int32_t opt) {
	int		flags;
	u_int32_t	r;

	r = li_connect_phase1(c, 0);
	if (r != ERR_CODE_OK)
		return r;
	if (c->connectState == Connected)
		return ERR_CODE_OK;

	assert(c->connectState == NotConnected);

	c->connectFamily = TcpFamily;

	memset(&c->serv_addr.inet.addr, 0, sizeof(c->serv_addr.inet.addr));
	c->serv_addr.inet.addr.sin_family = AF_INET;
	c->serv_addr.inet.addr.sin_addr.s_addr = (server && *server != '*' ) ? inet_addr(server) : htonl(INADDR_ANY);

	if ( c->serv_addr.inet.addr.sin_addr.s_addr == INADDR_NONE ) {
		struct hostent *host;

		host = gethostbyname(server);
		if ( host && host->h_addrtype == AF_INET ) {
			memcpy(&c->serv_addr.inet.addr.sin_addr.s_addr, host->h_addr_list[0],
					sizeof(c->serv_addr.inet.addr.sin_addr.s_addr));
		} else {
			return ERR_CODE_HOST_UNKNOWN;
		}
	}

	c->serv_addr.inet.addr.sin_port = htons(port);

	if ((c->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		return ERR_CODE_CONNECT_ERR;

	c->serv_addr.inet.port = port;
	c->serv_addr.inet.server = c->sp_alloc(NULL, strlen(server) + 1);
	strcpy(c->serv_addr.inet.server, server);

	c->connectState = ConnectInProgress;

	flags = 1;
	if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)) < 0) {
		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

	if (opt & LIBIPROTO_OPT_TCP_NODELAY) {
		if (setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags)) < 0) {
			c->connectState = ConnectionError;
			return ERR_CODE_CONNECT_ERR;
		}
	}

#ifdef SO_REUSEPORT
	if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEPORT, &flags, sizeof(flags)) < 0) {
		/*
		 * CentOS release 6.4 has only SO_REUSEPORT macros but
		 * not an underlying setsockopt
		 */
		if (errno != ENOPROTOOPT) {
			c->connectState = ConnectionError;
			return ERR_CODE_CONNECT_ERR;
		}
	}
#endif

	return li_connect_phase2(c, opt);
}

u_int32_t
li_uconnect(struct iproto_connection_t *c, const char *path, u_int32_t opt) {
	u_int32_t	r;

	r = li_connect_phase1(c, 0);
	if (r != ERR_CODE_OK)
		return r;
	if (c->connectState == Connected)
		return ERR_CODE_OK;

	assert(c->connectState == NotConnected);

	c->connectFamily = UnixFamily;

	memset(&c->serv_addr.un.addr, 0, sizeof(c->serv_addr.un.addr));
	c->serv_addr.un.addr.sun_family = AF_UNIX;
	strcpy(c->serv_addr.un.addr.sun_path, path);

	if ((c->fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return ERR_CODE_CONNECT_ERR;

	c->serv_addr.un.path = c->sp_alloc(NULL, strlen(path) + 1);
	strcpy(c->serv_addr.un.path, path);

	c->connectState = ConnectInProgress;

	return li_connect_phase2(c, opt);
}

u_int32_t
li_connect_timeout(struct iproto_connection_t *c, const char *server, int port, u_int32_t opt, u_int32_t timeout)
{
	u_int32_t r;

	if (!(opt & LIBIPROTO_OPT_NONBLOCK))
		return ERR_CODE_CONNECT_NON_BLOCK;

	r = li_connect(c, server, port, opt);
	if (r != ERR_CODE_CONNECT_IN_PROGRESS)
		return r;

	r = li_connect_phase1(c, timeout);
	if (r == ERR_CODE_CONNECT_IN_PROGRESS)
		return ERR_CODE_OPERATION_TIMEOUT;

	return r;
}

u_int32_t
li_uconnect_timeout(struct iproto_connection_t *c, const char *path, u_int32_t opt, u_int32_t timeout)
{
	u_int32_t r;

	if (!(opt & LIBIPROTO_OPT_NONBLOCK))
		return ERR_CODE_CONNECT_NON_BLOCK;

	r = li_uconnect(c, path, opt);
	if (r != ERR_CODE_CONNECT_IN_PROGRESS)
		return r;

	r = li_connect_phase1(c, timeout);
	if (r == ERR_CODE_CONNECT_IN_PROGRESS)
		return ERR_CODE_OPERATION_TIMEOUT;

	return r;
}

int
li_get_fd(struct iproto_connection_t *c) {
	if (c->connectState == ConnectInProgress || c->connectState == Connected)
		return c->fd;
	return -1;
}

const char *
li_get_addr(struct iproto_connection_t *c) {
	if (c->connectState == NotConnected)
		return NULL;

	switch (c->connectFamily) {
	case TcpFamily:
		return c->serv_addr.inet.server;
	case UnixFamily:
		return c->serv_addr.un.path;
	default:
		abort();
	}
}

static void
freeData(struct iproto_request_t *r) {
	if (r->dataResponsibility) {
		if (r->c->reqap)
			memory_arena_decr_refcount(r->dataArena);
		else
			r->c->sp_alloc(r->dataSend, 0);
	}

	if (r->readArena)
		memory_arena_decr_refcount(r->readArena);

	if (r->reqArena)
		memory_arena_decr_refcount(r->reqArena);
	else
		r->c->sp_alloc(r, 0);
}

void
li_close(struct iproto_connection_t *c) {
	if (c->fd >= 0)
		close(c->fd);

	c->fd = -1;
	c->connectState = NotConnected;

	switch (c->connectFamily) {
	case TcpFamily:
		if (c->serv_addr.inet.server)
			c->sp_alloc(c->serv_addr.inet.server, 0);
		break;
	case UnixFamily:
		if (c->serv_addr.un.path)
			c->sp_alloc(c->serv_addr.un.path, 0);
		break;
	default:
		abort();
	}
	memset(&c->serv_addr, 0, sizeof(c->serv_addr));

	mh_foreach(_sp_request, c->requestHash, k)
		freeData(mh_sp_request_value(c->requestHash, k));

	mh_sp_request_clear(c->requestHash);

	TAILQ_INIT(&c->sendList);
	TAILQ_INIT(&c->recvList);

	if (c->readArena) {
		memory_arena_decr_refcount(c->readArena);
		c->readArena = NULL;
	}

	c->nReqInProgress = 0;
}

void
li_free(struct iproto_connection_t *c) {
	if (c->connectState != NotConnected)
		li_close(c);

	mh_sp_request_destroy(c->requestHash);

	if (c->iovSend)
		c->sp_alloc(c->iovSend, 0);

	if (c->readArena)
		memory_arena_decr_refcount(c->readArena);
	if (c->reqArena)
		memory_arena_decr_refcount(c->reqArena);

	c->sp_alloc(c, 0);
}

LiConnectionState
li_io_state(struct iproto_connection_t *c) {
	LiConnectionState	state;

	switch(c->connectState) {
		case Connected:
			state = LI_CONNECTED;
			if (c->iovSendLength > 0 || !TAILQ_EMPTY(&c->sendList))
				state |= LI_WANT_WRITE;
			if (li_n_requests_in_progress(c) > 0)
				state |= LI_WANT_READ;
			break;

		case 	ConnectInProgress:
			state = LI_CONNECT_IN_PROGRESS | LI_WANT_WRITE;
			break;
		case 	ConnectionError:
			state = LI_CONNECT_ERROR;
			break;
		default:
			assert(NotConnected == c->connectState);
			state = LI_NOT_CONNECTED;
	}

	return state;
}

u_int32_t
li_n_requests(struct iproto_connection_t *c) {
	return mh_size(c->requestHash);
}

u_int32_t
li_n_requests_in_progress(struct iproto_connection_t *c) {
	assert(mh_size(c->requestHash) >= c->nReqInProgress);
	return c->nReqInProgress;
}

struct iproto_request_t*
li_req_ushard_init(struct iproto_connection_t* c, u_int16_t msg_code, u_int16_t ushard_id, void *data, size_t size) {
	struct iproto_request_t*	r;

	if (c->reqap) {
		if (c->reqArena == NULL || (c->reqArena->arenaSize - c->reqArena->arenaEnd) < sizeof(*r)) {
			if (c->reqArena)
				memory_arena_decr_refcount(c->reqArena);
			c->reqArena = map_get_arena(c->reqap, sizeof(*r));
			memory_arena_incr_refcount(c->reqArena);
		}

		r = (struct iproto_request_t*)(c->reqArena->data + c->reqArena->arenaEnd);
		c->reqArena->arenaEnd += sizeof(*r);
		r->reqArena = c->reqArena;
		memory_arena_incr_refcount(r->reqArena);
	} else {
		r = c->sp_alloc(NULL, sizeof(*r));
		if (!r)
			return NULL;
		r->reqArena = NULL;
	}

	r->c = c;
	r->state = ERR_CODE_REQUEST_IN_PROGRESS;

	r->dataSendSize = (data && size) ? size : 0;

	r->headerSend.data_len = size;
	r->headerSend.sync = ++c->mirrorCnt;
	r->headerSend.msg_code = msg_code;
	r->headerSend.shard_id = ushard_id;
	r->dataSend = data;
	r->assocData = NULL;

	mh_sp_request_put(c->requestHash, r->headerSend.sync, r, NULL);

	r->readArena = NULL;
	r->dataRecv = NULL;

	TAILQ_INSERT_TAIL(&c->sendList, r, link);
	c->nReqInProgress++;

	r->dataResponsibility = false;

	return r;
}

struct iproto_request_t*
li_req_ushard_init_copy(struct iproto_connection_t* c, u_int16_t msg_code, u_int16_t ushard_id, void *data, size_t size) {
	void 				*cdata;
	struct memory_arena_t		*arena = NULL;
	struct iproto_request_t		*request;

	if (size == 0)
		return li_req_init(c, msg_code, data, size);

	if (c->reqap) {
		if (c->reqArena == NULL || (c->reqArena->arenaSize - c->reqArena->arenaEnd) < size) {
			if (c->reqArena)
				memory_arena_decr_refcount(c->reqArena);
			c->reqArena = map_get_arena(c->reqap, size);
			memory_arena_incr_refcount(c->reqArena);
		}

		arena = c->reqArena;
		cdata = c->reqArena->data + c->reqArena->arenaEnd;
		c->reqArena->arenaEnd += size;
		memory_arena_incr_refcount(arena);
	} else {
		cdata = c->sp_alloc(NULL, size);

		if (!cdata)
			return NULL;
	}

	memcpy(cdata, data, size);
	request = li_req_ushard_init(c, msg_code, ushard_id, cdata, size);

	if (!request) {
		if (arena)
			memory_arena_decr_refcount(arena);
		else
			c->sp_alloc(cdata, 0);
	}

	request->dataResponsibility = true;
	request->dataArena = arena;

	return request;
}

struct iproto_request_t*
li_req_init(struct iproto_connection_t* c, u_int32_t msg_code, void *data, size_t size) {
	return li_req_ushard_init(c, (u_int16_t)msg_code, (u_int16_t)(msg_code>>16), data, size);
}

struct iproto_request_t*
li_req_init_copy(struct iproto_connection_t* c, u_int32_t msg_code, void *data, size_t size) {
	return li_req_ushard_init_copy(c, (u_int16_t)msg_code, (u_int16_t)(msg_code>>16), data, size);
}

void
li_req_set_assoc_data(struct iproto_request_t *r, void *data) {
	r->assocData = data;
}

void*
li_req_get_assoc_data(struct iproto_request_t *r) {
	return r->assocData;
}

u_int32_t
li_req_state(struct iproto_request_t *r) {
	if (r->state == ERR_CODE_REQUEST_READY) {
		if (r->c->has4errcode && r->headerRecv->header.data_len >= sizeof(u_int32_t))
			return r->headerRecv->header_retcode.ret_code;
		else
			return ERR_CODE_REQUEST_READY;
	}

	return r->state;
}

void*
li_req_response_data(struct iproto_request_t* r, size_t *size) {
	*size = 0;

	if (r->state == ERR_CODE_REQUEST_READY) {
		if (r->c->has4errcode && r->headerRecv->header.data_len >= sizeof(u_int32_t)) {
			*size = r->headerRecv->header.data_len - sizeof(u_int32_t);
			return r->dataRecv + sizeof(u_int32_t);
		} else {
			*size = r->headerRecv->header.data_len;
			return r->dataRecv;
		}
	}

	return NULL;
}

void*
li_req_request_data(struct iproto_request_t* r, size_t *size) {
	*size = r->dataSendSize;
	return r->dataSend;
}

void
li_req_free(struct iproto_request_t *r) {
	u_int32_t	k;

	if ((k = mh_sp_request_get(r->c->requestHash, r->headerSend.sync)) != mh_end(r->c->requestHash))
		mh_sp_request_del(r->c->requestHash, k);

	if (r->state == ERR_CODE_REQUEST_IN_PROGRESS)
		TAILQ_REMOVE(&r->c->sendList, r, link);

	if (r->state == ERR_CODE_REQUEST_READY)
		TAILQ_REMOVE(&r->c->recvList, r, link);

	freeData(r);
}

u_int32_t
li_write(struct iproto_connection_t *c) {
	int 	r;
	bool	doLoop = false;

	if (c->connectState == ConnectionError)
		return ERR_CODE_CONNECT_ERR;

	do {
		if (c->iovSendLength == 0) {
			struct iproto_request_t	*req;
			int			nToSend = li_n_requests_in_progress(c) * 2;

			if (nToSend > c->iovSendLengthMax) {
				c->iovSendLengthMax = nToSend;
				c->iovSend = c->iovptr = c->sp_alloc(c->iovSend, c->iovSendLengthMax * sizeof(struct iovec));
			} else {
				c->iovptr = c->iovSend;
			}

			c->iovSendLength = 0;

			TAILQ_FOREACH(req, &c->sendList, link) {
				req->state = ERR_CODE_REQUEST_IN_SEND;

				c->iovSend[c->iovSendLength].iov_base = &req->headerSend;
				c->iovSend[c->iovSendLength].iov_len = sizeof(req->headerSend);
				c->iovSendLength++;
				if (req->dataSendSize > 0) {
					c->iovSend[c->iovSendLength].iov_base = req->dataSend;
					c->iovSend[c->iovSendLength].iov_len = req->dataSendSize;
					c->iovSendLength++;
				}
			}

			TAILQ_INIT(&c->sendList);

			doLoop = false;
		} else {
			doLoop = true;
		}

		if (c->iovSendLength == 0)
			return ERR_CODE_NOTHING_TO_DO;

		while(c->iovSendLength > 0) {
			if ((r = writev(c->fd, c->iovptr, (c->iovSendLength < IOV_MAX) ? c->iovSendLength : IOV_MAX)) <= 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
					return ERR_CODE_OK;
				} else {
					c->connectState = ConnectionError;
					return ERR_CODE_CONNECT_ERR;
				}
			}

			while(c->iovSendLength > 0) {
				if (c->iovptr->iov_len > r ) {
					c->iovptr->iov_base = ((char*)(c->iovptr->iov_base)) + r;
					c->iovptr->iov_len -= r;
					return ERR_CODE_OK;
				} else {
					r -= c->iovptr->iov_len;
					c->iovptr++;
					c->iovSendLength--;
				}
			}
		}

	} while(doLoop);

	return ERR_CODE_NOTHING_TO_DO;
}

u_int32_t
li_read(struct iproto_connection_t *c) {

	if (c->connectState == ConnectionError)
		return ERR_CODE_CONNECT_ERR;

begin:
	for(;;) {
		ssize_t 	r;

		if (li_n_requests_in_progress(c) == 0) {
			if (c->readArena) {
				memory_arena_decr_refcount(c->readArena);
				c->readArena = NULL;
			}

			return ERR_CODE_NOTHING_TO_DO;
		}

		if (c->readArena == NULL) {
			c->readArena = map_get_arena(c->rap, c->neededSize);
			memory_arena_incr_refcount(c->readArena);
		} else if ((c->readArena->arenaSize - c->readArena->arenaEnd) < c->neededSize) {
			struct memory_arena_t	*oldarena = c->readArena;
			size_t			newsize = c->neededSize + (oldarena->arenaEnd - oldarena->arenaBegin);

			c->readArena = map_get_arena(c->rap, newsize);
			memory_arena_incr_refcount(c->readArena);

			if (oldarena->arenaEnd - oldarena->arenaBegin > 0) {
				memcpy(c->readArena->data, oldarena->data + oldarena->arenaBegin,
						oldarena->arenaEnd - oldarena->arenaBegin);
				c->readArena->arenaEnd = oldarena->arenaEnd - oldarena->arenaBegin;
			}

			memory_arena_decr_refcount(oldarena);
		}

		r = read(c->fd, c->readArena->data + c->readArena->arenaEnd,
			 	c->readArena->arenaSize - c->readArena->arenaEnd);

		if (r <= 0) {
			if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
				if (c->readArena->arenaEnd == c->readArena->arenaBegin) {
					memory_arena_decr_refcount(c->readArena);
					c->readArena = NULL;
				}

				return ERR_CODE_OK;
			}

			c->connectState = ConnectionError;
			return ERR_CODE_CONNECT_ERR;
		}

		c->readArena->arenaEnd += r;

		while(c->readArena->arenaEnd - c->readArena->arenaBegin >= sizeof(struct iproto)) {
			struct iproto		*header = (struct iproto*)
								(c->readArena->data + c->readArena->arenaBegin);
			struct iproto_request_t	*request;
			u_int32_t		k;

			if (c->readArena->arenaEnd - c->readArena->arenaBegin <
			    		sizeof(struct iproto) + header->data_len) {
				c->neededSize = MAX(MINNEEDEDSIZE, (sizeof(struct iproto) + header->data_len) -
								(c->readArena->arenaEnd - c->readArena->arenaBegin));

				goto begin;
			}

			if (header->msg_code == 0xff06) {
				c->readArena->arenaBegin += sizeof(*header);
				c->readArena->arenaBegin += header->data_len;
				continue;
			}

			k = mh_sp_request_get(c->requestHash, header->sync);
			if (k == mh_end(c->requestHash)) {
				c->connectState = ConnectionError;
				return ERR_CODE_PROTO_ERR;
			}
			request = mh_sp_request_value(c->requestHash, k);
			assert(header->sync == request->headerSend.sync);

			request->headerRecv = (union iproto_any_header*)header;

			c->readArena->arenaBegin += sizeof(*header);
			if (header->data_len > 0) {
				request->dataRecv = c->readArena->data + c->readArena->arenaBegin;
				c->readArena->arenaBegin += header->data_len;
			}

			memory_arena_incr_refcount(c->readArena);
			request->readArena = c->readArena;
			request->state = ERR_CODE_REQUEST_READY;
			TAILQ_INSERT_TAIL(&c->recvList, request, link);
			assert(c->nReqInProgress > 0);
			c->nReqInProgress--;
		}

		c->neededSize = MINNEEDEDSIZE;
	}

	return ERR_CODE_OK;
}

static void
li_gettime(struct timespec *ts)
{
#ifdef __MACH__
	struct timeval tv;

	gettimeofday(&tv, NULL);
	TIMEVAL_TO_TIMESPEC(&tv, ts);
#else
	clock_gettime(CLOCK_MONOTONIC, ts);
#endif
}

u_int32_t
li_write_timeout(struct iproto_connection_t *c, u_int32_t timeout)
{
	struct timespec start_ts, ts;
	u_int32_t spent = 0;
	u_int32_t r;

	li_gettime(&start_ts);

	for (;;) {
		r = li_write(c);
		if (r != ERR_CODE_OK)
			return r;

		li_gettime(&ts);
		spent = TIMESPEC_DIFF(ts, start_ts);
		if (spent >= timeout)
			return ERR_CODE_OPERATION_TIMEOUT;

		r = libpoll(c->fd, POLLOUT, timeout - spent);
		if (r & POLLERR) {
			c->connectState = ConnectionError;
			return ERR_CODE_CONNECT_ERR;
		}
		if (!(r & POLLOUT))
			return ERR_CODE_OPERATION_TIMEOUT;

	}

	return ERR_CODE_OK;
}

u_int32_t
li_read_timeout(struct iproto_connection_t *c, u_int32_t timeout)
{
	struct timespec start_ts, ts;
	u_int32_t spent = 0;
	u_int32_t r;

	li_gettime(&start_ts);

	for (;;) {
		r = li_read(c);
		if (r != ERR_CODE_OK)
			return r;

		li_gettime(&ts);
		spent = TIMESPEC_DIFF(ts, start_ts);
		if (spent >= timeout)
			return ERR_CODE_OPERATION_TIMEOUT;

		r = libpoll(c->fd, POLLIN, timeout - spent);
		if (r & POLLERR) {
			c->connectState = ConnectionError;
			return ERR_CODE_CONNECT_ERR;
		}
		if (!(r & POLLIN))
			return ERR_CODE_OPERATION_TIMEOUT;

	}

	return ERR_CODE_OK;
}

struct iproto_request_t*
li_get_ready_reqs(struct iproto_connection_t *c) {
	struct iproto_request_t	*req = NULL;

	if ((req = TAILQ_FIRST(&c->recvList)) != NULL) {
		TAILQ_REMOVE(&c->recvList, req, link);
		assert(req->state == ERR_CODE_REQUEST_READY);
	}

	return req;
}


