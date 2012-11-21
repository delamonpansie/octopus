/*
 * Copyright (C) 2011, 2012 Mail.RU
 * Copyright (C) 2011, 2012 Yuriy Vostrikov
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

#ifndef IPROTO_DEF_H
#define IPROTO_DEF_H

#include <sys/types.h>
/*
 * This file is shared between libiproto and octopus, C and Objective-C
 */


/*
 * struct iproto_header and struct iproto_header_retcode
 * share common prefix {msg_code, len, sync}
 */

struct iproto {
	u_int32_t msg_code;
	u_int32_t data_len;						/* not including header */
	u_int32_t sync;
	u_int8_t data[];
} __attribute__((packed));

struct iproto_retcode {
	u_int32_t msg_code;
	u_int32_t data_len;
	u_int32_t sync;
	u_int32_t ret_code;
	u_int8_t data[];
} __attribute__((packed));

union iproto_any_header {
	struct iproto header;
	struct iproto_retcode header_retcode;
};

#define ERROR_CODES(_)					    \
	_(ERR_CODE_OK,                    0x00000000, "ok") \
	_(ERR_CODE_NONMASTER,             0x00000102, "non master connection, but it should be") \
	_(ERR_CODE_ILLEGAL_PARAMS,        0x00000202, "illegal parametrs") \
	_(ERR_CODE_BAD_UID,               0x00000302, "uid not from this storage range") \
	_(ERR_CODE_NODE_IS_RO,            0x00000401, "node is marked as read-only") \
	_(ERR_CODE_NODE_IS_NOT_LOCKED,    0x00000501, "node isn't locked") \
	_(ERR_CODE_NODE_IS_LOCKED,        0x00000601, "node is locked") \
	_(ERR_CODE_MEMORY_ISSUE,          0x00000701, "some memory issues") \
	_(ERR_CODE_BAD_INTEGRITY,         0x00000802, "bad graph integrity") \
	_(ERR_CODE_UNSUPPORTED_COMMAND,   0x00000a02, "unsupported command") \
	/* gap due to silverproxy */					\
	_(ERR_CODE_CANNOT_REGISTER,       0x00001801, "can't register new user") \
	_(ERR_CODE_CANNOT_INIT_ALERT_ID,  0x00001a01, "can't generate alert id") \
	_(ERR_CODE_CANNOT_DEL,            0x00001b02, "can't del node") \
	_(ERR_CODE_USER_NOT_REGISTERED,   0x00001c02, "user isn't registered") \
	/* silversearch error codes */					\
	_(ERR_CODE_SYNTAX_ERROR,          0x00001d02, "syntax error in query") \
	_(ERR_CODE_WRONG_FIELD,           0x00001e02, "unknown field") \
	_(ERR_CODE_WRONG_NUMBER,          0x00001f02, "number value is out of range") \
	_(ERR_CODE_DUPLICATE,             0x00002002, "insert already existing object") \
	_(ERR_CODE_UNSUPPORTED_ORDER,     0x00002202, "can not order result") \
	_(ERR_CODE_MULTIWRITE,            0x00002302, "multiple to update/delete") \
	_(ERR_CODE_NOTHING,               0x00002400, "nothing to do (not an error)") \
	_(ERR_CODE_UPDATE_ID,             0x00002502, "id's update") \
	_(ERR_CODE_WRONG_VERSION,         0x00002602, "unsupported version of protocol") \
	/* other generic error codes */					\
	_(ERR_CODE_UNKNOWN_ERROR,         0x00002702, "unknown error") \
	_(ERR_CODE_NODE_NOT_FOUND,	  0x00003102, "node isn't found")      \
	_(ERR_CODE_NODE_FOUND,		  0x00003702, "node is found") \
	_(ERR_CODE_INDEX_VIOLATION,	  0x00003802, "some index violation occur") \
	_(ERR_CODE_NO_SUCH_NAMESPACE,	  0x00003902, "there is no such namespace") \
	_(ERR_CODE_NAUTH_OK,              0x00004000, "non authoritative ok") \
	_(ERR_CODE_REDIRECT,              0x00004102, "redirect")	\
	_(ERR_CODE_LEADER_UNKNOW,	  0x00004202, "leader unknown")


/* Macros to define enum and corresponding strings. */
#ifndef ENUM_INITIALIZER
#  define ENUM_DEF(s, v, d...) s = v,
#  define ENUM_INITIALIZER(define) { define(ENUM_DEF) }
#endif
#ifndef ENUM_STR_INITIALISER
#  define ENUM_STR_DEF(s, v, d...) [s] = #s,
#  define ENUM_STR_INITIALISER(define) { define(ENUM_STR_DEF) }
#endif

extern void errcode_add_desc(u_int32_t errcode, char *desc);
extern char* errcode_desc(u_int32_t errcode);
#ifndef ERRCODE_ADD
#  define ERRCODE_DESCRIPTION(s, v, d ...) errcode_add_desc((v), (d));
#  define ERRCODE_STRINGIFY(s, v) errcode_add_desc((v), #s);
#  define ERRCODE_ADD(how, define) do { define(how) } while(0)
#endif

#endif
