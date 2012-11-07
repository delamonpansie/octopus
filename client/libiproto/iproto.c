/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2012 Teodor Sigaev
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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

#include <config.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <limits.h>
#include <netinet/in.h>
#include <sys/param.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdbool.h>

#include <client/libiproto/libiproto.h>
#include <third_party/queue.h>
#include <iproto_def.h>

#ifndef offsetof
#define offsetof(type, field)   ((int) (uintptr_t)&((type *)0)->field)
#endif   /* offsetof */

#define MH_STATIC
#define mh_name _sp_request
#define mh_key_t u_int32_t
#define mh_val_t struct iproto_request_t*
#define mh_hash(a) ({ (a); })
#define mh_eq(a, b) ({ *(mh_key_t *)((a) + sizeof(mh_val_t)) == (b); })
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
	char					*server;
	struct sockaddr_in  			serv_addr;
	int					port;
	memalloc				sp_alloc;
	struct mhash_t				*requestHash;
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
	struct memory_arena_t		*reqArena;

	union iproto_any_header		*headerRecv;
	char				*dataRecv;

	struct memory_arena_t		*readArena;

	TAILQ_ENTRY(iproto_request_t)   link;

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
libpoll(int fd, int event) {
	struct pollfd	pfd;
	int		ret;

	pfd.fd = fd;
	pfd.events = event;
	pfd.revents = 0;

	ret = poll( &pfd, 1, 0);
	if (ret < 0 || (pfd.revents & (POLLHUP | POLLNVAL | POLLERR)) != 0)
		return (pfd.revents | POLLERR);

	return pfd.revents;
}

u_int32_t
li_connect(struct iproto_connection_t *c, char *server, int port, u_int32_t opt) {
	int 		flags;
	u_int32_t	r;

	if (c->fd >= 0) {
		switch(c->connectState) {
			case Connected:
				return ERR_CODE_ALREADY_CONNECTED;
			case ConnectionError:
				return ERR_CODE_CONNECT_ERR;
			case ConnectInProgress:
				r = libpoll(c->fd, POLLOUT);

				if (r & POLLERR) {
					c->connectState = ConnectionError;
					return ERR_CODE_CONNECT_ERR;
				}

				if (r & POLLOUT) {
					c->connectState = Connected;

					return ERR_CODE_OK; 
				}

				return ERR_CODE_CONNECT_IN_PROGRESS;

			case NotConnected:
			default:
				abort();
		}
	}

	assert(c->connectState == NotConnected);

	c->nonblock = (opt & LIBIPROTO_OPT_NONBLOCK) ? true : false;
	c->has4errcode = (opt & LIBIPROTO_OPT_HAS_4BYTE_ERRCODE) ? true : false;

	memset(&c->serv_addr, 0, sizeof(c->serv_addr));
	c->serv_addr.sin_family = AF_INET;
	c->serv_addr.sin_addr.s_addr = (server && *server != '*' ) ? inet_addr(server) : htonl(INADDR_ANY);

	if ( c->serv_addr.sin_addr.s_addr == INADDR_NONE ) {
		struct hostent *host;

		host = gethostbyname(server);
		if ( host && host->h_addrtype == AF_INET ) {
			memcpy(&c->serv_addr.sin_addr.s_addr, host->h_addr_list[0],
					sizeof(c->serv_addr.sin_addr.s_addr));
		} else {
			return ERR_CODE_HOST_UNKNOWN;
		}
	}

	c->serv_addr.sin_port = htons(port);

	if ((c->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		return ERR_CODE_CONNECT_ERR;

	c->connectState = ConnectInProgress;

	c->port = port;
	c->server = c->sp_alloc(NULL, strlen(server) + 1);
	strcpy(c->server, server);

	if (c->nonblock && ((flags=fcntl(c->fd,F_GETFL,0)) == -1 || fcntl(c->fd,F_SETFL,flags|O_NDELAY) < 0)) {
		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

	flags = 1;
	if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)) < 0) {
		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

#ifdef SO_REUSEPORT
	if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEPORT, &flags, sizeof(flags)) < 0) {
		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}
#endif

	if ( connect(c->fd, (struct sockaddr *) &(c->serv_addr), sizeof(struct sockaddr_in)) < 0 ) {
		if ( errno == EINPROGRESS || errno == EALREADY )
			return ERR_CODE_CONNECT_IN_PROGRESS;

		c->connectState = ConnectionError;
		return ERR_CODE_CONNECT_ERR;
	}

	c->connectState = Connected;
	return ERR_CODE_OK;
}

int
li_get_fd(struct iproto_connection_t *c) {
	if (c->connectState == ConnectInProgress || c->connectState == Connected)
		return c->fd;
	return -1;
}

static void                 
freeData(struct iproto_request_t *r) {
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

	if (c->server)
		c->sp_alloc(c->server, 0);
	c->server = NULL;
	c->port = 0;
	memset(&c->serv_addr, 0, sizeof(c->serv_addr));

	mh_foreach(c->requestHash, k)
		freeData(mh_sp_request_value(c->requestHash, k));

	mh_clear(c->requestHash);

	TAILQ_INIT(&c->sendList);
	TAILQ_INIT(&c->recvList);

	c->nReqInProgress = 0;
}

void
li_free(struct iproto_connection_t *c) {
	if (c->connectState != NotConnected)
		li_close(c);

	mh_destroy(c->requestHash);

	if (c->iovSend)
		c->sp_alloc(c->iovSend, 0);

	if (c->readArena)
		memory_arena_decr_refcount(c->readArena);
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
li_req_init(struct iproto_connection_t* c, u_int32_t msg_code, void *data, size_t size) {
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
	r->dataSend = data;

	mh_sp_request_put(c->requestHash, r->headerSend.sync, r, NULL);

	r->readArena = NULL;
	r->dataRecv = NULL;

	TAILQ_INSERT_TAIL(&c->sendList, r, link);
	c->nReqInProgress++;

	return r;
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
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
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
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
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

			if (c->readArena->arenaEnd - c->readArena->arenaBegin < 
			    		sizeof(struct iproto) + header->data_len) {
				c->neededSize = MAX(MINNEEDEDSIZE, (sizeof(struct iproto) + header->data_len) - 
								(c->readArena->arenaEnd - c->readArena->arenaBegin));

				goto begin;
			}

			do {
				u_int32_t			k;

				k = mh_sp_request_get(c->requestHash, header->sync);
				assert(k != mh_end(c->requestHash));
				request = mh_sp_request_value(c->requestHash, k);
			} while(0);

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

struct iproto_request_t*
li_get_ready_reqs(struct iproto_connection_t *c) {
	struct iproto_request_t	*req = NULL;

	if ((req = TAILQ_FIRST(&c->recvList)) != NULL) {
		TAILQ_REMOVE(&c->recvList, req, link);
		assert(req->state == ERR_CODE_REQUEST_READY);
	}

	return req;
}


