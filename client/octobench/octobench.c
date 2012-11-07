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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <poll.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>

/************************************************************************************/
/*-
 * Copyright (c) 1990, 1993
 *  The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * Posix rand_r function added May 1999 by Wes Peters <wes@softweyr.com>.
 */

#define BL_RAND_MAX     0x7fffffff

static int
do_rand(unsigned long *ctx)
{
/*
 * Compute x = (7^5 * x) mod (2^31 - 1)
 * wihout overflowing 31 bits:
 *      (2^31 - 1) = 127773 * (7^5) + 2836
 * From "Random number generators: good ones are hard to find",
 * Park and Miller, Communications of the ACM, vol. 31, no. 10,
 * October 1988, p. 1195.
 */
	long hi, lo, x;

	/* Can't be initialized with 0, so use another value. */
	if (*ctx == 0)
		*ctx = 123459876;
	hi = *ctx / 127773;
	lo = *ctx % 127773;
	x = 16807 * lo - 2836 * hi;
	if (x < 0)
		x += 0x7fffffff;
	return ((*ctx = x) % ((unsigned long)BL_RAND_MAX + 1));
}

static int
blrand_r(unsigned int *ctx)
{
	unsigned long val = (unsigned long) *ctx;
	int r = do_rand(&val);

	*ctx = (unsigned int) val;
	return (r);
}

/************************************************************************************/

#include <client/libiproto/libiproto.h>

static u_int64_t	nReqInPacket = 10;
static u_int64_t	nPackets = 1000;
static u_int64_t	nActiveConnections = 10;
static u_int64_t	nConnections = 10;
static u_int64_t	nSuccess = 0;
static u_int64_t	nTotal = 0;
static pthread_mutex_t	mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t	cond  = PTHREAD_COND_INITIALIZER;
static bool		ignoreFatal = false;
static bool		printRequest = false;

#define		OCTO_PING	(0xff00) /* XXX src/iproto.m:const uint32_t msg_ping = 0xff00; */
#define		BOX_INSERT	(13)
#define		BOX_SELECT	(17)

static u_int16_t	messageType = OCTO_PING;
static u_int32_t	minId = 0,
			maxId = 1000;

static u_int32_t	errstat[ 256 ];

static 	char		*server = NULL;
int			port = -1;

static void
signalMain(u_int32_t nOk, u_int32_t nProceed) {
	pthread_mutex_lock(&mutex);
	nActiveConnections--;
	nSuccess += nOk;
	nTotal += nProceed;
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&mutex);
}

static void*
my_alloc(void *ptr, size_t size) {
	if (ptr == NULL)
		return malloc(size);
	if (size == 0) {
		free(ptr);
		return NULL;
	}
	return realloc(ptr, size);
}

static int
octopoll(int fd, int event) {
	struct pollfd	pfd;
	int		ret;

	pfd.fd = fd;
	pfd.events = event;
	pfd.revents = 0;

	ret = poll(&pfd, 1, -1);
	if (ret < 0 || (pfd.revents & (POLLHUP | POLLNVAL | POLLERR)) != 0)
		return (pfd.revents | POLLERR);

	return pfd.revents;
}

typedef struct AllocatedRequestBody {
	struct AllocatedRequestBody	*next;
	/* data follows */
} AllocatedRequestBody;

static inline void *
popAllocatedRequestBody(AllocatedRequestBody **stack, size_t size) {
	AllocatedRequestBody	*p;

	if (*stack) {
		p = *stack;
		*stack = (*stack)->next;
	} else {
		p = malloc(size + sizeof(*p));
	}

	return ((char*)p) + sizeof(*p);
}

static inline void
pushAllocatedRequestBody(AllocatedRequestBody **stack, char *ptr) {
	AllocatedRequestBody	*p;

	if (ptr) {
		p = (AllocatedRequestBody*)(ptr - sizeof(*p));
		p->next = *stack;
		*stack = p;
	}
}

static inline void*
generateRequestBody(AllocatedRequestBody **stack, int workerId, unsigned int *seed, size_t *size) {
	void *ptr = NULL;

	*size = 0;

#define GEN_RND_BETWEEN(mn, mx)	(mn + (mx - mn) * ( ((double)blrand_r(seed)) / ((double)BL_RAND_MAX) ))

	switch(messageType) {
		case OCTO_PING:
			break;
		case BOX_INSERT:
			{
				struct {
					u_int32_t	object_space;
					u_int32_t	flags;
					u_int32_t	n;
					u_int8_t	len;
					u_int32_t	id;
				} __attribute__((packed)) *ps, s = {0, 0, 1, 4, 0};

				ps = popAllocatedRequestBody(stack, sizeof(*ps));
				memcpy(ps, &s, sizeof(*ps));
				ps->id = GEN_RND_BETWEEN(minId, maxId);
				ptr = ps;
				*size = sizeof(*ps);
			}
			break;
		case BOX_SELECT:
			{
				struct {
					u_int32_t	object_space;
					u_int32_t	index;
					u_int32_t	offset;
					u_int32_t 	limit;
					u_int32_t	n;
					u_int32_t	cardinality;
					u_int8_t	len;
					u_int32_t	id;
				} __attribute__((packed)) *ps, s = {0, 0, 0, 0xffffffff, 1, 1, 4, 0}; 

				ps = popAllocatedRequestBody(stack, sizeof(*ps));
				memcpy(ps, &s, sizeof(*ps));
				ps->id = GEN_RND_BETWEEN(minId, maxId);
				ptr = ps;
				*size = sizeof(*ps);
			}
			break;
		default:
			abort();
	}

	return ptr;
}

void*
worker(void *arg) {
	int				idWorker = (int)(uintptr_t)arg;
	unsigned int			rndseed = idWorker;
	struct memory_arena_pool_t*	rap = map_alloc(my_alloc, 2, 64*1024);
	struct memory_arena_pool_t*	reqap = map_alloc(my_alloc, 64, 64*1024);
	struct iproto_connection_t*	conn = li_conn_init(my_alloc, rap, reqap);
	u_int32_t			errcode;
	int				fd;
	u_int32_t			nSended = 0, nOk = 0, nGet = 0;
	bool				needToSend = false;
	int				nRequests = nReqInPacket * nPackets;
	AllocatedRequestBody		*stack = NULL;
	size_t				size;
	u_int32_t			flags = LIBIPROTO_OPT_NONBLOCK;

	if (messageType != OCTO_PING)
		flags |= LIBIPROTO_OPT_HAS_4BYTE_ERRCODE;	

	while((errcode = li_connect(conn, server, port, flags)) == ERR_CODE_CONNECT_IN_PROGRESS)
		octopoll(li_get_fd(conn), POLLOUT);

	if (errcode != ERR_CODE_OK) {
		fprintf(stderr,"li_connect fails: %08x\n", errcode);
		exit(1);
	}

	fd = li_get_fd(conn);

	while(!(nOk >= nRequests && nGet == nSended)) {
		int state;

		state = octopoll(fd, POLLIN | ((needToSend || nOk < nRequests) ? POLLOUT : 0));
		if (state & POLLERR) {
			fprintf(stderr,"poll fails: %s\n", strerror(errno));
			exit(1);
		}

		if (state & POLLIN) {
			struct iproto_request_t	*request;	

			errcode = li_read(conn);

			if (!(errcode == ERR_CODE_NOTHING_TO_DO || errcode == ERR_CODE_OK)) {
				fprintf(stderr,"li_read fails: %08x\n", errcode);
				exit(1);
			}

			while((request = li_get_ready_reqs(conn)) != NULL) {

				errcode = li_req_state(request);

				if (errcode != ERR_CODE_REQUEST_READY && (errcode & LIBIPROTO_ERR_CODE_FLAG)) {
					fprintf(stderr,"request fails: %08x\n", errcode);
					exit(1);
				}

				if (errcode == ERR_CODE_OK || errcode == ERR_CODE_REQUEST_READY)
					nOk++;
				nGet++;

				if (ignoreFatal == false && errcode & 0x02) {
					fprintf(stderr,"octopus returns fatal error: %08x\n", errcode);
					exit(1);
				}

				__sync_fetch_and_add(errstat + ((errcode >> 8) & 0xff), 1);		
		
				pushAllocatedRequestBody(&stack, li_req_request_data(request, &size));
				li_req_free(request);
			}
		}

		if (state & POLLOUT) {
			if (nOk < nRequests && (nGet + 4 * nReqInPacket) > nSended) {
				int i;

				for(i=0; i<nReqInPacket; i++) {
					void	*body;
					size_t	size;

					body = generateRequestBody(&stack, idWorker, &rndseed, &size); 
					li_req_init(conn, messageType, body, size);
				}
				
				nSended += nReqInPacket;

				needToSend = true;
			} 

			if (needToSend) {
				switch((errcode = li_write(conn))) {
					case ERR_CODE_NOTHING_TO_DO:
						needToSend = false;
					case ERR_CODE_OK:
						break;
					default:
						fprintf(stderr,"li_write fails: %08x\n", errcode);
						exit(1);
				}
			}
		}
	}

	signalMain(nOk, nGet);
	li_close(conn);
	li_free(conn);
	map_free(rap);

	return NULL;
}

extern char *optarg;
extern int opterr;

static void
usage() {
	/*    ################################################################################ */
	puts("octobench -s HOST -p PORT"); 
	puts("   [-n NPACKETS] [-m NREQUESTS_IN_PACKETS] [-c NCONNECTION]"); 
	puts("   [-t (ping|box_insert|box_select)]");
	puts("   [-i MINID] [-I MAXID] -- min/max random id");
	puts("   [-F]                  -- ignore fatal error from db");
	puts("Defaults:");
	printf("    -n %"PRIu64" -m %"PRIu64" -c %"PRIu64"\n    -t ping", nPackets, nReqInPacket, nConnections); 
	printf("    -i %u -I %u\n", minId, maxId); 
	exit(1);
}

#define	OPT_ERR(cond)	if (!(cond))	{						\
	fprintf(stderr, "Input option check fails: " #cond "\n");	\
	exit(1);													\
}

static inline double
timediff(struct timeval *begin, struct timeval *end) {
    return ((double)( end->tv_sec - begin->tv_sec )) + ( (double)( end->tv_usec-begin->tv_usec ) ) / 1.0e+6;
}

int
main(int argc, char* argv[]) {
	int 			i;
	struct timeval 		begin, end;
	double			elapsed;
	pthread_t		tid;

	nConnections = nActiveConnections;

	while((i=getopt(argc,argv,"s:p:n:m:c:t:Fi:I:h")) != EOF) {
		switch(i) {
			case 's':
				server = strdup(optarg);
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'm':
				nReqInPacket = atoi(optarg);
				break;
			case 'n':
				nPackets = atoi(optarg);
				break;
			case 'c':
				nActiveConnections = nConnections = atoi(optarg);
				break;
			case 't':
				if (strcmp("ping", optarg) == 0)
					messageType = OCTO_PING;
				else if (strcmp("box_insert", optarg) == 0)
					messageType = BOX_INSERT;
				else if (strcmp("box_select", optarg) == 0)
					messageType = BOX_SELECT;
				else
					usage();
				break;
			case 'i':
				minId = strtoul(optarg, NULL, 0);
				break;
			case  'I':
				maxId = strtoul(optarg, NULL, 0);
				break;
			case 'F':
				ignoreFatal = true;
				break;
			case 'h':
			default:
				usage();
		}
	}

	OPT_ERR(minId < maxId);

	if (server==NULL || port <= 0)
		usage();

	gettimeofday(&begin,NULL);
	pthread_mutex_lock(&mutex);
	for(i=0; i<nConnections; i++) {
		int err;

		if ((err = pthread_create(&tid, NULL, worker, (void*)(uintptr_t)(i+1))) != 0) {
			printf("pthread_create fails: %s\n", strerror(err));
			exit(1);
		}
	}

	while(nActiveConnections > 0)
		pthread_cond_wait(&cond, &mutex);
	pthread_mutex_unlock(&mutex);
	gettimeofday(&end,NULL);

	elapsed = timediff(&begin, &end);

	printf("Elapsed time: %.3f secs\n", elapsed);
	printf("Number of OK requests: %.3f\n", ((double)nSuccess)); 
	printf("RPS: %.3f\n", ((double)nSuccess) / elapsed );
	printf("Number of ALL requests: %.3f\n", ((double)nTotal)); 
	printf("RPS: %.3f\n", ((double)nTotal) / elapsed );

	for(i=0; i<256; i++)
		if (errstat[i] > 0)
			printf("N number of %08x: %d\n", i, errstat[i]);

	return 0;
}
