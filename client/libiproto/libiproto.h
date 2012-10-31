/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Teodor Sigaev
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

#ifndef LIBIPROTO_H
#define LIBIPROTO_H

#include <sys/types.h>

/**********************************************************************************
 *                               BLOCKNIG USAGE                                   *
 **********************************************************************************
 *
 * struct iproto_connection_t* conn = li_conn_init(realloc, ...);
 *  
 * if ((err = li_connect(conn, "127.0.0.1", 98876, LIBIPROTO_OPT_HAS_4BYTE_ERRCODE)) != ERR_CODE_OK)
 * 		exit(1);
 *
 * struct iproto_request_t *request = li_req_init(conn, MSGCODE, data, len);
 * if ((err = li_write(conn)) != ERR_CODE_NOTHING_TO_DO)
 *		exit(1);
 * if ((err = li_read(conn)) != ERR_CODE_NOTHING_TO_DO) {
 * 		exit(1);
 *
 * ...
 **********************************************************************************
 *                               NONBLOCKNIG USAGE                                *
 **********************************************************************************
 * int fd = li_get_fd(conn);
 *
 * while(42) { 
 * 	state = poll(fd);
 *
 *	if (state & POLLIN) {
 *		iproto_request_t	*request;
 *		errcode = li_read(conn);
 *
 *		while((request = li_get_ready_reqs(conn)) != NULL) {
 *			...
 *			li_req_free(request);
 *		}
 *	}
 *
 *	if (state & POLLOUT) {
 *		while(some .. ) {
 *			iproto_request_t	*request = li_req_init(...);
 *		}
 *		errcode = li_write(conn);
 *	}
 * } 
 **********************************************************************************/


/*
 * memory allocation callback.
 * void* memalloc(void* ptr, size_t *size);
 * it should follow following rules:
 *  - memalloc(NULL, size > 0)  malloc equivalent
 *  - memalloc(ptr, size > 0)   realloc
 *  - memalloc(ptr, 0)          free
 */
typedef void* (*memalloc)(void*, size_t);

struct read_arena_pool_t;

struct read_arena_pool_t* 	rap_alloc(memalloc sp_alloc, u_int32_t maxArenas, size_t readArenaSize);
void   				rap_free(struct read_arena_pool_t *rap);

#define LIBIPROTO_ERR_CODE_FLAG		(0x04)	

#define LIBIPROTO_ERROR_CODES(_)                                           \
    _(ERR_CODE_HOST_UNKNOWN,             ((0x01) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x02), "hosk unknown")	\
    _(ERR_CODE_CONNECT_ERR,              ((0x02) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x02), "connection error")    \
    _(ERR_CODE_PROTO_ERR,                ((0x03) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x02), "protocol error")    \
    _(ERR_CODE_NOTHING_TO_DO,            ((0x04) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "nothing to do")    \
    _(ERR_CODE_ALREADY_CONNECTED,        ((0x05) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "already connected")    \
    _(ERR_CODE_CONNECT_IN_PROGRESS,      ((0x06) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "connect is in progress")    \
    _(ERR_CODE_REQUEST_IN_PROGRESS,      ((0x07) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "request is in progress")    \
    _(ERR_CODE_REQUEST_IN_SEND,          ((0x08) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "request is in sending")    \
    _(ERR_CODE_REQUEST_IN_RECV,          ((0x09) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x01), "request is on recieving")    \
    _(ERR_CODE_REQUEST_READY,            ((0x0a) << 16) | (LIBIPROTO_ERR_CODE_FLAG | 0x00), "request is ready")

#define LIBIPROTOENUM_MEMBER(s, v, d...) s = v,
#define LIBIPROTOENUM(enum_name, enum_members) enum enum_name {enum_members(LIBIPROTOENUM_MEMBER) enum_name##_MAX}

LIBIPROTOENUM(libIPROTO_error_codes, LIBIPROTO_ERROR_CODES);

#ifndef ERR_CODE_OK
#define ERR_CODE_OK 0x00000000
#endif

#define LIBIPROTO_OPT_NONE		0x00
#define LIBIPROTO_OPT_NONBLOCK		0x01
#define LIBIPROTO_OPT_HAS_4BYTE_ERRCODE	0x02

struct iproto_connection_t;
struct iproto_request_t;

struct iproto_connection_t*	li_conn_init(memalloc sp_alloc, struct read_arena_pool_t *rap);
u_int32_t			li_connect(struct iproto_connection_t *c, char *server, int port, u_int32_t opt);
int				li_get_fd(struct iproto_connection_t *c);
void				li_close(struct iproto_connection_t *c);
void				li_free(struct iproto_connection_t *c);

u_int32_t			li_n_requests(struct iproto_connection_t *c);
u_int32_t			li_n_requests_in_progress(struct iproto_connection_t *c);

u_int32_t			li_write(struct iproto_connection_t *c);
u_int32_t			li_read(struct iproto_connection_t *c);

struct iproto_request_t*	li_get_ready_reqs(struct iproto_connection_t *c);

struct iproto_request_t*	li_req_init(struct iproto_connection_t* c, 
					    u_int32_t msg_code, void *data, size_t size);
u_int32_t			li_req_state(struct iproto_request_t* r);
void*				li_req_response_data(struct iproto_request_t* r, size_t *size);
void*               		li_req_request_data(struct iproto_request_t* r, size_t *size);
void				li_req_free(struct iproto_request_t* r);

#endif
